import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3tables from 'aws-cdk-lib/aws-s3tables';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as athena from 'aws-cdk-lib/aws-athena';
import * as redshift from 'aws-cdk-lib/aws-redshift';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import { Construct } from 'constructs';

interface WebEventsDataLakeStackProps extends cdk.StackProps {
  sourceS3Bucket?: string;
  kinesisStreamName?: string;
}

export class WebEventsDataLakeStack extends cdk.Stack {
  public readonly dataLakeBucket: s3.Bucket;
  public readonly tableBucket: s3tables.CfnTableBucket;
  public readonly webEventsTable: s3tables.CfnTable;
  public readonly sessionMetricsTable: s3tables.CfnTable;
  public readonly glueDatabase: glue.CfnDatabase;
  public readonly glueJobKinesis: glue.CfnJob;
  public readonly glueJobS3: glue.CfnJob;
  public readonly kinesisStream: kinesis.Stream;
  public readonly lambdaProcessor: lambda.Function;
  public readonly redshiftCluster: redshift.Cluster;

  constructor(scope: Construct, id: string, props?: WebEventsDataLakeStackProps) {
    super(scope, id, props);

    // VPC for Redshift and other resources
    const vpc = new ec2.Vpc(this, 'DataLakeVPC', {
      maxAzs: 2,
      natGateways: 1,
      subnetConfiguration: [
        {
          cidrMask: 24,
          name: 'Public',
          subnetType: ec2.SubnetType.PUBLIC,
        },
        {
          cidrMask: 24,
          name: 'Private',
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
        },
      ],
    });

    // S3 Table Bucket for Iceberg tables
    this.tableBucket = new s3tables.CfnTableBucket(this, 'WebEventsTableBucket', {
      tableBucketName: `peek-web-events-tables-${this.account}`,
    });

    // Traditional S3 Bucket for auxiliary data (scripts, temp files, etc.)
    this.dataLakeBucket = new s3.Bucket(this, 'WebEventsDataLake', {
      bucketName: `peek-web-events-datalake-${this.account}`,
      versioned: true,
      encryption: s3.BucketEncryption.KMS_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: [
        {
          id: 'AuxiliaryDataLifecycle',
          enabled: true,
          transitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: cdk.Duration.days(30),
            },
          ],
        },
      ],
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Auxiliary data can be recreated
    });

    // S3 Bucket for Glue Scripts
    const glueScriptsBucket = new s3.Bucket(this, 'GlueScripts', {
      bucketName: `peek-glue-scripts-${this.account}`,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Deploy Glue scripts to S3
    new s3deploy.BucketDeployment(this, 'DeployGlueScripts', {
      sources: [s3deploy.Source.asset('../glue')],
      destinationBucket: glueScriptsBucket,
      destinationKeyPrefix: 'scripts/',
    });

    // S3 Bucket for Batch Operations reports
    const batchOperationsReportsBucket = new s3.Bucket(this, 'BatchOperationsReports', {
      bucketName: `peek-batch-operations-reports-${this.account}`,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: [
        {
          id: 'BatchReportsLifecycle',
          enabled: true,
          transitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: cdk.Duration.days(30),
            },
            {
              storageClass: s3.StorageClass.GLACIER,
              transitionAfter: cdk.Duration.days(90),
            },
          ],
          expiration: cdk.Duration.days(365), // Delete after 1 year
        },
      ],
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // IAM Role for S3 Batch Operations
    const batchOperationsRole = new iam.Role(this, 'S3BatchOperationsRole', {
      assumedBy: new iam.ServicePrincipal('batchoperations.s3.amazonaws.com'),
      roleName: 'S3BatchOperationsRole',
      inlinePolicies: {
        BatchOperationsPolicy: new iam.PolicyDocument({
          statements: [
            // Read inventory manifest and source objects
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                's3:GetObject',
                's3:GetObjectVersion',
                's3:ListBucket',
              ],
              resources: [
                'arn:aws:s3:::peek-inventory-bucket',
                'arn:aws:s3:::peek-inventory-bucket/*',
                'arn:aws:s3:::prod-backup-web-events',
                'arn:aws:s3:::prod-backup-web-events/*',
              ],
            }),
            // Restore objects from Glacier
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                's3:RestoreObject',
              ],
              resources: [
                'arn:aws:s3:::prod-backup-web-events/*',
              ],
            }),
            // Write batch operations reports
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                's3:PutObject',
                's3:PutObjectAcl',
              ],
              resources: [
                batchOperationsReportsBucket.bucketArn,
                `${batchOperationsReportsBucket.bucketArn}/*`,
              ],
            }),
          ],
        }),
      },
    });

    // IAM Role for Glue Jobs
    const glueRole = new iam.Role(this, 'GlueJobRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
      ],
      inlinePolicies: {
        GlueJobPolicy: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                's3:GetObject',
                's3:PutObject',
                's3:DeleteObject',
                's3:ListBucket',
              ],
              resources: [
                this.dataLakeBucket.bucketArn,
                `${this.dataLakeBucket.bucketArn}/*`,
                glueScriptsBucket.bucketArn,
                `${glueScriptsBucket.bucketArn}/*`,
              ],
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'kinesis:DescribeStream',
                'kinesis:GetShardIterator',
                'kinesis:GetRecords',
                'kinesis:ListShards',
              ],
              resources: ['*'],
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'glue:GetTable',
                'glue:GetPartition',
                'glue:GetPartitions',
                'glue:CreatePartition',
                'glue:UpdatePartition',
                'glue:BatchCreatePartition',
                'glue:GetDatabase',
                'glue:GetTables',
              ],
              resources: ['*'],
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'logs:CreateLogGroup',
                'logs:CreateLogStream',
                'logs:PutLogEvents',
              ],
              resources: [`arn:aws:logs:${this.region}:${this.account}:*`],
            }),
          ],
        }),
      },
    });

    // Kinesis Stream for real-time events
    this.kinesisStream = new kinesis.Stream(this, 'WebEventsStream', {
      streamName: 'peek-web-events-stream',
      shardCount: 5,
      retentionPeriod: cdk.Duration.days(7),
      encryption: kinesis.StreamEncryption.KMS,
      encryptionKey: undefined, // Use default KMS key
    });

    // Lambda function for real-time Kinesis processing
    this.lambdaProcessor = new lambda.Function(this, 'KinesisProcessor', {
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'index.lambda_handler',
      code: lambda.Code.fromAsset('../lambda/kinesis-processor'),
      timeout: cdk.Duration.minutes(5),
      memorySize: 512,
      environment: {
        BUCKET_NAME: this.dataLakeBucket.bucketName,
        GLUE_DATABASE: 'peek_web_events',
        TABLE_BUCKET_ARN: this.tableBucket.attrArn,
        WEB_EVENTS_TABLE: `${this.tableBucket.attrArn}/namespaces/analytics/tables/web_events`,
        SESSION_METRICS_TABLE: `${this.tableBucket.attrArn}/namespaces/analytics/tables/session_metrics`,
      },
      reservedConcurrentExecutions: 100,
    });

    // S3 Tables Namespace (equivalent to database)
    const tablesNamespace = new s3tables.CfnNamespace(this, 'WebEventsNamespace', {
      tableBucketArn: this.tableBucket.attrArn,
      namespace: 'analytics',
    });

    // Main web events table (Iceberg format)
    this.webEventsTable = new s3tables.CfnTable(this, 'WebEventsTable', {
      tableBucketArn: this.tableBucket.attrArn,
      namespace: 'analytics',
      name: 'web_events',
      format: 'ICEBERG',
    });

    // Session metrics aggregation table
    this.sessionMetricsTable = new s3tables.CfnTable(this, 'SessionMetricsTable', {
      tableBucketArn: this.tableBucket.attrArn,
      namespace: 'analytics',
      name: 'session_metrics',
      format: 'ICEBERG',
    });

    // Glue Database for legacy compatibility and external table access
    this.glueDatabase = new glue.CfnDatabase(this, 'WebEventsDatabase', {
      catalogId: this.account,
      databaseInput: {
        name: 'peek_web_events',
        description: 'Database for web events clickstream data (Iceberg format)',
        parameters: {
          'classification': 'iceberg',
          'table_format': 'ICEBERG',
        },
      },
    });

    // Glue Job for Kinesis Stream Processing
    this.glueJobKinesis = new glue.CfnJob(this, 'KinesisProcessorJob', {
      name: 'peek-web-events-kinesis-processor',
      role: glueRole.roleArn,
      command: {
        name: 'gluestreaming',
        scriptLocation: `s3://${glueScriptsBucket.bucketName}/scripts/kinesis_processor.py`,
        pythonVersion: '3',
      },
      defaultArguments: {
        '--job-language': 'python',
        '--job-bookmark-option': 'job-bookmark-enable',
        '--enable-metrics': '',
        '--enable-spark-ui': 'true',
        '--spark-event-logs-path': `s3://${glueScriptsBucket.bucketName}/sparkHistoryLogs/`,
        '--enable-job-insights': 'false',
        '--enable-observability-metrics': 'true',
        '--OUTPUT_BUCKET': this.dataLakeBucket.bucketName,
        '--continuous-log-logGroup': `/aws-glue/jobs/logs-v2`,
        '--enable-continuous-cloudwatch-log': 'true',
      },
      executionProperty: {
        maxConcurrentRuns: 2,
      },
      glueVersion: '4.0',
      maxRetries: 0,
      timeout: 2880, // 48 hours
    });

    // Glue Job for Historical S3 Data Processing
    this.glueJobS3 = new glue.CfnJob(this, 'S3ProcessorJob', {
      name: 'peek-web-events-s3-processor',
      role: glueRole.roleArn,
      command: {
        name: 'glueetl',
        scriptLocation: `s3://${glueScriptsBucket.bucketName}/scripts/s3_processor.py`,
        pythonVersion: '3',
      },
      defaultArguments: {
        '--job-language': 'python',
        '--job-bookmark-option': 'job-bookmark-enable',
        '--enable-metrics': '',
        '--enable-spark-ui': 'true',
        '--spark-event-logs-path': `s3://${glueScriptsBucket.bucketName}/sparkHistoryLogs/`,
        '--enable-job-insights': 'false',
        '--enable-observability-metrics': 'true',
        '--OUTPUT_BUCKET': this.dataLakeBucket.bucketName,
        '--SOURCE_BUCKET': props?.sourceS3Bucket || 'your-source-bucket',
        '--continuous-log-logGroup': `/aws-glue/jobs/logs-v2`,
        '--enable-continuous-cloudwatch-log': 'true',
      },
      executionProperty: {
        maxConcurrentRuns: 1,
      },
      glueVersion: '4.0',
      maxRetries: 1,
      timeout: 2880, // 48 hours
      workerType: 'G.1X',
      numberOfWorkers: 10,
    });

    // Redshift cluster for structured analytics
    const redshiftSubnetGroup = new redshift.ClusterSubnetGroup(this, 'RedshiftSubnetGroup', {
      description: 'Subnet group for Redshift cluster',
      vpcSubnets: {
        subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
      },
      vpc,
    });

    this.redshiftCluster = new redshift.Cluster(this, 'WebEventsRedshift', {
      masterUser: {
        masterUsername: 'admin',
        masterPassword: cdk.SecretValue.unsafePlainText('TempPassword123!'), // Use Secrets Manager in production
      },
      vpc,
      subnetGroup: redshiftSubnetGroup,
      defaultDatabaseName: 'webevents',
      nodeType: redshift.NodeType.DC2_LARGE,
      numberOfNodes: 2,
      encrypted: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Athena Workgroup for querying
    const athenaWorkgroup = new athena.CfnWorkGroup(this, 'WebEventsWorkgroup', {
      name: 'peek-web-events-workgroup',
      description: 'Workgroup for web events analytics',
      state: 'ENABLED',
      workGroupConfiguration: {
        resultConfiguration: {
          outputLocation: `s3://${this.dataLakeBucket.bucketName}/athena-results/`,
          encryptionConfiguration: {
            encryptionOption: 'SSE_KMS',
          },
        },
        enforceWorkGroupConfiguration: true,
        publishCloudWatchMetrics: true,
        bytesScannedCutoffPerQuery: 10000000000, // 10GB limit for large analytics
        engineVersion: {
          selectedEngineVersion: 'Athena engine version 3',
        },
      },
    });

    // CloudWatch Dashboard for monitoring
    const dashboard = new cloudwatch.Dashboard(this, 'DataLakeDashboard', {
      dashboardName: 'WebEventsDataLake',
      widgets: [
        [new cloudwatch.GraphWidget({
          title: 'Kinesis Stream Metrics',
          left: [
            this.kinesisStream.metricIncomingRecords(),
            this.kinesisStream.metricIncomingBytes(),
          ],
          right: [
            this.lambdaProcessor.metricInvocations(),
            this.lambdaProcessor.metricErrors(),
          ],
        })],
      ],
    });

    // CloudWatch Log Groups for Glue Jobs
    new cdk.CfnOutput(this, 'DataLakeBucketName', {
      value: this.dataLakeBucket.bucketName,
      description: 'S3 bucket name for the data lake',
    });

    new cdk.CfnOutput(this, 'GlueDatabaseName', {
      value: this.glueDatabase.ref,
      description: 'Glue database name',
    });

    new cdk.CfnOutput(this, 'KinesisJobName', {
      value: this.glueJobKinesis.ref,
      description: 'Glue job name for Kinesis processing',
    });

    new cdk.CfnOutput(this, 'S3JobName', {
      value: this.glueJobS3.ref,
      description: 'Glue job name for S3 processing',
    });

    new cdk.CfnOutput(this, 'AthenaWorkgroupName', {
      value: athenaWorkgroup.ref,
      description: 'Athena workgroup for querying web events',
    });

    new cdk.CfnOutput(this, 'BatchOperationsRoleArn', {
      value: batchOperationsRole.roleArn,
      description: 'IAM role ARN for S3 Batch Operations Glacier restore jobs',
    });

    new cdk.CfnOutput(this, 'BatchReportsBucketName', {
      value: batchOperationsReportsBucket.bucketName,
      description: 'S3 bucket for Batch Operations job reports',
    });
  }
}