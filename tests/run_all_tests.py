"""
Comprehensive test runner for S3 Tables/Iceberg data lake migration test suite
Executes all test categories and generates detailed reports for enterprise QA standards.
"""

import pytest
import sys
import os
import json
import time
from datetime import datetime
from pathlib import Path
import subprocess
import argparse
from typing import Dict, List, Any


class TestSuiteRunner:
    """Comprehensive test suite runner with reporting and validation"""
    
    def __init__(self, base_path: str = None):
        """Initialize test runner"""
        self.base_path = Path(base_path) if base_path else Path(__file__).parent
        self.results = {}
        self.start_time = None
        self.end_time = None
        
        # Test categories and their configurations
        self.test_categories = {
            'unit': {
                'description': 'Unit Tests - Individual component testing',
                'path': 'unit',
                'critical': True,
                'timeout': 600,  # 10 minutes
                'test_files': [
                    'test_infrastructure.py',
                    'test_glue_etl.py', 
                    'test_lambda_processor.py',
                    'test_dbt_models.py',
                    'test_airflow_dag.py',
                    'test_schema_definitions.py',
                    'test_deployment_script.py'
                ]
            },
            'integration': {
                'description': 'Integration Tests - End-to-end pipeline testing',
                'path': 'integration',
                'critical': True,
                'timeout': 1200,  # 20 minutes
                'test_files': [
                    'test_end_to_end_pipeline.py'
                ]
            },
            'performance': {
                'description': 'Performance Tests - Large-scale processing and optimization',
                'path': 'performance', 
                'critical': False,
                'timeout': 1800,  # 30 minutes
                'test_files': [
                    'test_large_scale_processing.py'
                ]
            },
            'error_handling': {
                'description': 'Error Handling Tests - Failure scenarios and recovery',
                'path': 'error_handling',
                'critical': True,
                'timeout': 900,  # 15 minutes
                'test_files': [
                    'test_comprehensive_error_recovery.py'
                ]
            }
        }
    
    def run_test_category(self, category: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Run a specific test category"""
        print(f"\n{'='*60}")
        print(f"Running {config['description']}")
        print(f"{'='*60}")
        
        category_path = self.base_path / config['path']
        category_results = {
            'category': category,
            'description': config['description'],
            'critical': config['critical'],
            'start_time': datetime.now().isoformat(),
            'test_files': [],
            'total_tests': 0,
            'passed_tests': 0,
            'failed_tests': 0,
            'skipped_tests': 0,
            'errors': [],
            'warnings': [],
            'duration_seconds': 0,
            'status': 'pending'
        }
        
        start_time = time.time()
        
        try:
            # Run tests for each file in category
            for test_file in config['test_files']:
                test_file_path = category_path / test_file
                
                if not test_file_path.exists():
                    category_results['warnings'].append(f"Test file not found: {test_file}")
                    continue
                
                print(f"\n📋 Running {test_file}...")
                
                # Run pytest for this specific file
                cmd = [
                    sys.executable, '-m', 'pytest',
                    str(test_file_path),
                    '-v',
                    '--tb=short',
                    '--maxfail=10',
                    '--json-report',
                    '--json-report-file', str(category_path / f'{test_file}.json'),
                    f'--timeout={config["timeout"]}'
                ]
                
                try:
                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        timeout=config['timeout']
                    )
                    
                    # Parse results
                    json_report_path = category_path / f'{test_file}.json'
                    file_results = self._parse_pytest_json(json_report_path, test_file)
                    
                    category_results['test_files'].append(file_results)
                    category_results['total_tests'] += file_results['total_tests']
                    category_results['passed_tests'] += file_results['passed_tests']
                    category_results['failed_tests'] += file_results['failed_tests']
                    category_results['skipped_tests'] += file_results['skipped_tests']
                    
                    if file_results['failed_tests'] > 0:
                        category_results['errors'].extend(file_results['failures'])
                    
                    # Print immediate results
                    print(f"   ✅ Passed: {file_results['passed_tests']}")
                    print(f"   ❌ Failed: {file_results['failed_tests']}")
                    print(f"   ⏭️  Skipped: {file_results['skipped_tests']}")
                    
                except subprocess.TimeoutExpired:
                    error_msg = f"Test file {test_file} timed out after {config['timeout']} seconds"
                    category_results['errors'].append(error_msg)
                    print(f"   ⏰ TIMEOUT: {error_msg}")
                    
                except Exception as e:
                    error_msg = f"Error running test file {test_file}: {str(e)}"
                    category_results['errors'].append(error_msg)
                    print(f"   💥 ERROR: {error_msg}")
            
            # Determine category status
            if category_results['failed_tests'] == 0 and len(category_results['errors']) == 0:
                category_results['status'] = 'passed'
            elif category_results['failed_tests'] > 0:
                category_results['status'] = 'failed'
            else:
                category_results['status'] = 'error'
        
        except Exception as e:
            category_results['status'] = 'error'
            category_results['errors'].append(f"Category execution error: {str(e)}")
        
        finally:
            end_time = time.time()
            category_results['duration_seconds'] = round(end_time - start_time, 2)
            category_results['end_time'] = datetime.now().isoformat()
        
        return category_results
    
    def _parse_pytest_json(self, json_path: Path, test_file: str) -> Dict[str, Any]:
        """Parse pytest JSON report"""
        file_results = {
            'test_file': test_file,
            'total_tests': 0,
            'passed_tests': 0,
            'failed_tests': 0,
            'skipped_tests': 0,
            'failures': [],
            'duration_seconds': 0
        }
        
        try:
            if json_path.exists():
                with open(json_path, 'r') as f:
                    data = json.load(f)
                
                summary = data.get('summary', {})
                file_results['total_tests'] = summary.get('total', 0)
                file_results['passed_tests'] = summary.get('passed', 0)
                file_results['failed_tests'] = summary.get('failed', 0)
                file_results['skipped_tests'] = summary.get('skipped', 0)
                file_results['duration_seconds'] = data.get('duration', 0)
                
                # Extract failure details
                for test in data.get('tests', []):
                    if test.get('outcome') == 'failed':
                        file_results['failures'].append({
                            'test_name': test.get('nodeid', 'unknown'),
                            'error': test.get('call', {}).get('longrepr', 'No error details')
                        })
        
        except Exception as e:
            file_results['failures'].append(f"Error parsing results for {test_file}: {str(e)}")
        
        return file_results
    
    def run_all_tests(self, categories: List[str] = None) -> Dict[str, Any]:
        """Run all test categories or specified ones"""
        self.start_time = datetime.now()
        print(f"\n🚀 Starting Comprehensive S3 Tables/Iceberg Test Suite")
        print(f"Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Test Categories: {len(self.test_categories)}")
        
        # Determine which categories to run
        categories_to_run = categories if categories else list(self.test_categories.keys())
        
        # Run each category
        for category in categories_to_run:
            if category not in self.test_categories:
                print(f"⚠️  Unknown test category: {category}")
                continue
            
            config = self.test_categories[category]
            self.results[category] = self.run_test_category(category, config)
        
        self.end_time = datetime.now()
        
        # Generate comprehensive summary
        return self._generate_summary()
    
    def _generate_summary(self) -> Dict[str, Any]:
        """Generate comprehensive test summary"""
        total_duration = (self.end_time - self.start_time).total_seconds()
        
        summary = {
            'test_suite': 'S3 Tables/Iceberg Data Lake Migration',
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat(),
            'duration_seconds': round(total_duration, 2),
            'categories_run': len(self.results),
            'overall_status': 'pending',
            'critical_failures': [],
            'category_results': self.results,
            'totals': {
                'total_tests': 0,
                'passed_tests': 0,
                'failed_tests': 0,
                'skipped_tests': 0,
                'error_count': 0,
                'warning_count': 0
            },
            'recommendations': [],
            'enterprise_readiness': {
                'score': 0,
                'criteria': {}
            }
        }
        
        # Calculate totals and analyze results
        critical_failures = 0
        all_passed = True
        
        for category_name, category_result in self.results.items():
            # Accumulate totals
            summary['totals']['total_tests'] += category_result['total_tests']
            summary['totals']['passed_tests'] += category_result['passed_tests']
            summary['totals']['failed_tests'] += category_result['failed_tests']
            summary['totals']['skipped_tests'] += category_result['skipped_tests']
            summary['totals']['error_count'] += len(category_result['errors'])
            summary['totals']['warning_count'] += len(category_result['warnings'])
            
            # Check for critical failures
            if category_result['critical'] and category_result['status'] != 'passed':
                critical_failures += 1
                summary['critical_failures'].append({
                    'category': category_name,
                    'status': category_result['status'],
                    'failed_tests': category_result['failed_tests'],
                    'errors': len(category_result['errors'])
                })
                all_passed = False
            elif category_result['status'] != 'passed':
                all_passed = False
        
        # Determine overall status
        if critical_failures > 0:
            summary['overall_status'] = 'critical_failure'
        elif not all_passed:
            summary['overall_status'] = 'failure'
        else:
            summary['overall_status'] = 'success'
        
        # Generate recommendations
        summary['recommendations'] = self._generate_recommendations(summary)
        
        # Calculate enterprise readiness score
        summary['enterprise_readiness'] = self._calculate_enterprise_readiness(summary)
        
        return summary
    
    def _generate_recommendations(self, summary: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on test results"""
        recommendations = []
        
        # Critical failures
        if summary['critical_failures']:
            recommendations.append("🚨 CRITICAL: Address all critical test failures before production deployment")
        
        # Test coverage
        total_tests = summary['totals']['total_tests']
        if total_tests < 100:
            recommendations.append(f"📈 Consider expanding test coverage (current: {total_tests} tests)")
        
        # Failure rate
        failed_tests = summary['totals']['failed_tests']
        if failed_tests > 0:
            failure_rate = (failed_tests / total_tests) * 100 if total_tests > 0 else 0
            if failure_rate > 5:
                recommendations.append(f"🔧 High failure rate ({failure_rate:.1f}%) - investigate and fix failing tests")
        
        # Performance tests
        if 'performance' in summary['category_results']:
            perf_result = summary['category_results']['performance']
            if perf_result['status'] != 'passed':
                recommendations.append("⚡ Performance tests not passing - validate system can handle 75GB+ datasets")
        
        # Error handling
        if 'error_handling' in summary['category_results']:
            error_result = summary['category_results']['error_handling']
            if error_result['status'] != 'passed':
                recommendations.append("🛡️ Error handling tests failing - ensure system resilience for production")
        
        # Integration tests
        if 'integration' in summary['category_results']:
            integration_result = summary['category_results']['integration']
            if integration_result['status'] != 'passed':
                recommendations.append("🔗 Integration tests failing - validate end-to-end data flow")
        
        # Add positive recommendations
        if summary['overall_status'] == 'success':
            recommendations.extend([
                "✅ All critical tests passing - system ready for production deployment",
                "🎯 Consider implementing continuous testing in CI/CD pipeline",
                "📊 Monitor system performance metrics in production environment"
            ])
        
        return recommendations
    
    def _calculate_enterprise_readiness(self, summary: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate enterprise readiness score"""
        criteria = {
            'unit_tests_passing': 0,
            'integration_tests_passing': 0,
            'performance_validated': 0,
            'error_handling_robust': 0,
            'deployment_validated': 0,
            'schema_validated': 0,
            'orchestration_tested': 0
        }
        
        # Evaluate each criterion
        for category_name, category_result in summary['category_results'].items():
            success = category_result['status'] == 'passed'
            
            if category_name == 'unit':
                criteria['unit_tests_passing'] = 25 if success else 0
                if 'test_infrastructure.py' in str(category_result):
                    criteria['deployment_validated'] += 10 if success else 0
                if 'test_schema_definitions.py' in str(category_result):
                    criteria['schema_validated'] += 10 if success else 0
                if 'test_airflow_dag.py' in str(category_result):
                    criteria['orchestration_tested'] += 10 if success else 0
            
            elif category_name == 'integration':
                criteria['integration_tests_passing'] = 25 if success else 0
            
            elif category_name == 'performance':
                criteria['performance_validated'] = 15 if success else 0
            
            elif category_name == 'error_handling':
                criteria['error_handling_robust'] = 15 if success else 0
        
        # Calculate total score
        total_score = sum(criteria.values())
        
        # Determine readiness level
        if total_score >= 90:
            readiness_level = 'production_ready'
        elif total_score >= 75:
            readiness_level = 'staging_ready'
        elif total_score >= 50:
            readiness_level = 'development_ready'
        else:
            readiness_level = 'not_ready'
        
        return {
            'score': total_score,
            'max_score': 100,
            'percentage': round((total_score / 100) * 100, 1),
            'level': readiness_level,
            'criteria': criteria
        }
    
    def print_summary_report(self, summary: Dict[str, Any]):
        """Print comprehensive summary report"""
        print(f"\n{'='*80}")
        print(f"🏁 S3 TABLES/ICEBERG DATA LAKE TEST SUITE SUMMARY")
        print(f"{'='*80}")
        
        # Overall status
        status_emoji = {
            'success': '✅',
            'failure': '⚠️',
            'critical_failure': '🚨'
        }
        
        print(f"\n📊 OVERALL STATUS: {status_emoji.get(summary['overall_status'], '❓')} {summary['overall_status'].upper()}")
        print(f"⏱️  Duration: {summary['duration_seconds']:.1f} seconds")
        print(f"📈 Enterprise Readiness: {summary['enterprise_readiness']['percentage']:.1f}% ({summary['enterprise_readiness']['level'].replace('_', ' ').title()})")
        
        # Test totals
        totals = summary['totals']
        print(f"\n📋 TEST SUMMARY:")
        print(f"   Total Tests: {totals['total_tests']}")
        print(f"   ✅ Passed: {totals['passed_tests']}")
        print(f"   ❌ Failed: {totals['failed_tests']}")
        print(f"   ⏭️  Skipped: {totals['skipped_tests']}")
        print(f"   💥 Errors: {totals['error_count']}")
        print(f"   ⚠️  Warnings: {totals['warning_count']}")
        
        # Category breakdown
        print(f"\n📂 CATEGORY BREAKDOWN:")
        for category_name, category_result in summary['category_results'].items():
            status_icon = '✅' if category_result['status'] == 'passed' else '❌'
            critical_marker = '🔥' if category_result['critical'] else '  '
            
            print(f"   {status_icon} {critical_marker} {category_name.upper()}: "
                  f"{category_result['passed_tests']}/{category_result['total_tests']} passed "
                  f"({category_result['duration_seconds']:.1f}s)")
        
        # Critical failures
        if summary['critical_failures']:
            print(f"\n🚨 CRITICAL FAILURES:")
            for failure in summary['critical_failures']:
                print(f"   • {failure['category'].upper()}: {failure['failed_tests']} failed tests, {failure['errors']} errors")
        
        # Recommendations
        if summary['recommendations']:
            print(f"\n💡 RECOMMENDATIONS:")
            for i, recommendation in enumerate(summary['recommendations'], 1):
                print(f"   {i}. {recommendation}")
        
        # Enterprise readiness details
        readiness = summary['enterprise_readiness']
        print(f"\n🏢 ENTERPRISE READINESS CRITERIA:")
        for criterion, score in readiness['criteria'].items():
            max_score = 25 if criterion in ['unit_tests_passing', 'integration_tests_passing'] else 15
            percentage = (score / max_score) * 100 if max_score > 0 else 0
            status = '✅' if percentage >= 80 else '⚠️' if percentage >= 50 else '❌'
            print(f"   {status} {criterion.replace('_', ' ').title()}: {score}/{max_score} ({percentage:.0f}%)")
        
        print(f"\n{'='*80}")


def main():
    """Main entry point for test runner"""
    parser = argparse.ArgumentParser(description='S3 Tables/Iceberg Data Lake Test Suite Runner')
    parser.add_argument('--categories', nargs='+', 
                       choices=['unit', 'integration', 'performance', 'error_handling'],
                       help='Specific test categories to run (default: all)')
    parser.add_argument('--output', '-o', help='Output file for JSON results')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    # Initialize test runner
    runner = TestSuiteRunner()
    
    # Run tests
    try:
        summary = runner.run_all_tests(categories=args.categories)
        
        # Print summary
        runner.print_summary_report(summary)
        
        # Save results to file if specified
        if args.output:
            output_path = Path(args.output)
            with open(output_path, 'w') as f:
                json.dump(summary, f, indent=2)
            print(f"\n💾 Results saved to: {output_path}")
        
        # Exit with appropriate code
        if summary['overall_status'] == 'success':
            sys.exit(0)
        elif summary['overall_status'] == 'failure':
            sys.exit(1)
        else:  # critical_failure
            sys.exit(2)
    
    except KeyboardInterrupt:
        print(f"\n\n⚠️  Test run interrupted by user")
        sys.exit(130)
    
    except Exception as e:
        print(f"\n\n💥 Test runner error: {str(e)}")
        sys.exit(3)


if __name__ == "__main__":
    main()