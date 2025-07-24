#!/usr/bin/env python3
"""
Advanced usage example for PGDN-SUI library
"""

from pgdn_sui import ValidatorScanner
import json

def main():
    # Initialize scanner with custom timeout
    scanner = ValidatorScanner(timeout=5)

    # Example validator hostnames
    hostnames = [
        "validator1.sui.io",
        "validator2.sui.io",
        "validator3.sui.io"
    ]

    results = {}

    for hostname in hostnames:
        print(f"\n{'='*60}")
        print(f"Scanning {hostname}")
        print('='*60)

        # Phase 1: Discovery
        print("Phase 1: Discovery and Classification")
        phase1_result = scanner.execute_phase(hostname, 1)

        if phase1_result['status'] != 'success':
            print(f"ERROR Phase 1 failed for {hostname}: {phase1_result['error']}")
            continue

        node_type = phase1_result['data']['classification']['node_type']
        print(f"Node type: {node_type}")

        # Phase 2: Deep analysis
        print("Phase 2: Version extraction and health check")
        phase2_result = scanner.execute_phase(hostname, 2, phase1_result['data'])

        if phase2_result['status'] == 'success':
            print(f"SUCCESS Complete scan successful for {hostname}")
        else:
            print(f"WARNING Phase 2 failed for {hostname}: {phase2_result['error']}")

        # Store results
        results[hostname] = {
            'phase1': phase1_result,
            'phase2': phase2_result,
            'node_type': node_type
        }

    # Summary
    print(f"\n{'='*60}")
    print("BATCH SCAN SUMMARY")
    print('='*60)

    for hostname, result in results.items():
        node_type = result['node_type']
        phase1_success = result['phase1']['status'] == 'success'
        phase2_success = result['phase2']['status'] == 'success'

        status = "SUCCESS" if phase1_success and phase2_success else "WARNING"
        print(f"{status} {hostname}: {node_type}")

    # Save results to file
    with open('batch_scan_results.json', 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\nResults saved to batch_scan_results.json")

if __name__ == "__main__":
    main()
