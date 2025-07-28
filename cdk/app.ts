#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { WebEventsDataLakeStack } from './lib/web-events-data-lake-stack';

const app = new cdk.App();

new WebEventsDataLakeStack(app, 'WebEventsDataLakeStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});