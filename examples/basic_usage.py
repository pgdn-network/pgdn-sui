#!/usr/bin/env python3
"""
Basic usage example for PGDN-SUI library
"""

from pgdn_sui import ValidatorScanner

def main():
    # Initialize scanner
    scanner = ValidatorScanner()

    # Example validator hostname
    hostname = "validator.sui.io"

    # Get Phase 1 execution plan
    print("Getting Phase 1 execution plan...")
    plan = scanner.get_plan(hostname, phase=1)
    print(f"Phase 1 has {len(plan['steps'])} steps")

    # Execute Phase 1 steps individually
    print("\nExecuting Phase 1 steps...")
    previous_data = []

    for step_info in plan['steps']:
        step_num = step_info['step']
        step_name = step_info['name']

        print(f"Executing step {step_num}: {step_name}")
        result = scanner.execute_step(hostname, 1, step_num, previous_data)

        if result['status'] == 'success':
            print(f"  SUCCESS Step {step_num} completed successfully")
            previous_data.append(result)
        else:
            print(f"  ERROR Step {step_num} failed: {result['error']}")
            break

    # Execute full Phase 1 (alternative approach)
    print("\nExecuting full Phase 1...")
    full_result = scanner.execute_phase(hostname, 1)

    if full_result['status'] == 'success':
        print("SUCCESS Phase 1 completed successfully")
        node_type = full_result['data']['classification']['node_type']
        print(f"Node type: {node_type}")
    else:
        print(f"ERROR Phase 1 failed: {full_result['error']}")

if __name__ == "__main__":
    main()
