#!/usr/bin/env python3
"""
GraphQL Extractor
Handles GraphQL extraction from Sui nodes
"""

import logging
import requests
from typing import Dict
from ..models import SuiDataResult
from ..utils import calculate_gini_coefficient

logger = logging.getLogger(__name__)


class GraphqlExtractor:
    """Handles GraphQL extraction from Sui nodes"""
    
    # GraphQL queries for deep intelligence
    GRAPHQL_QUERIES = {
        "network_info": """
        query NetworkInfo {
            chainIdentifier
            epoch {
                epochId
                referenceGasPrice
                startTimestamp
                endTimestamp
                validatorSet {
                    totalCount
                }
            }
            protocolConfig {
                protocolVersion
            }
        }
        """,
        
        "validator_info": """
        query ValidatorInfo($first: Int) {
            epoch {
                validatorSet(first: $first) {
                    totalCount
                    nodes {
                        name
                        address {
                            address
                        }
                        nextEpochStake
                        votingPower
                        commissionRate
                        apy
                    }
                }
            }
        }
        """,
    }
    
    def __init__(self, timeout: int = 10, config: dict = None):
        self.timeout = timeout
        self.config = config or {}
        self.logger = logger

    async def extract_graphql_intelligence_async(self, result: SuiDataResult, ip: str) -> bool:
        """Extract intelligence via GraphQL"""
        graphql_endpoints = [
            f"http://{ip}:9000/graphql",
            f"https://{ip}:9000/graphql",
            f"http://{ip}:3030/graphql",
            f"https://{ip}:3030/graphql",
        ]
        
        for endpoint in graphql_endpoints:
            try:
                # Test GraphQL introspection
                introspection_query = {
                    "query": "query { __schema { queryType { name } } }"
                }
                
                response = requests.post(endpoint, json=introspection_query, timeout=self.timeout)
                
                if response.status_code == 200:
                    data = response.json()
                    if "data" in data and "__schema" in data["data"]:
                        result.graphql_available = True
                        
                        # Execute intelligence queries
                        await self._execute_graphql_intelligence_queries(result, endpoint)
                        return True
                        
            except Exception as e:
                self.logger.debug(f"GraphQL endpoint {endpoint} failed: {e}")
                continue
        
        return False

    async def _execute_graphql_intelligence_queries(self, result: SuiDataResult, endpoint: str):
        """Execute GraphQL queries for intelligence extraction"""
        for query_name, query in self.GRAPHQL_QUERIES.items():
            try:
                variables = {}
                if query_name == "validator_info":
                    variables = {"first": 100}  # Limit validators returned
                
                response = requests.post(endpoint, json={
                    "query": query,
                    "variables": variables
                }, timeout=self.timeout)
                
                if response.status_code == 200:
                    data = response.json()
                    if "data" in data:
                        await self._process_graphql_intelligence(result, query_name, data["data"])
                        
            except Exception as e:
                self.logger.debug(f"GraphQL query {query_name} failed: {e}")

    async def _process_graphql_intelligence(self, result: SuiDataResult, query_name: str, data: Dict):
        """Process GraphQL intelligence data"""
        try:
            if query_name == "network_info":
                if "chainIdentifier" in data:
                    result.chain_identifier = data["chainIdentifier"]
                
                if "epoch" in data and data["epoch"]:
                    epoch_data = data["epoch"]
                    result.current_epoch = epoch_data.get("epochId")
                    result.reference_gas_price = epoch_data.get("referenceGasPrice")
                    
                    if "validatorSet" in epoch_data:
                        result.validator_count = epoch_data["validatorSet"].get("totalCount")
                
                if "protocolConfig" in data:
                    result.protocol_version = str(data["protocolConfig"].get("protocolVersion"))
                    
            elif query_name == "validator_info":
                if "epoch" in data and "validatorSet" in data["epoch"]:
                    validator_set = data["epoch"]["validatorSet"]
                    result.validator_count = validator_set.get("totalCount")
                    
                    validators = validator_set.get("nodes", [])
                    if validators:
                        await self._analyze_validator_set_from_graphql(result, validators)
                        
        except Exception as e:
            result.extraction_errors.append(f"graphql_intelligence_processing_error_{query_name}: {str(e)}")

    async def _analyze_validator_set_from_graphql(self, result: SuiDataResult, validators: list):
        """Analyze validator set from GraphQL data"""
        try:
            stakes = []
            total_stake = 0
            
            for validator in validators:
                next_epoch_stake = validator.get("nextEpochStake")
                if next_epoch_stake:
                    try:
                        stake_amount = int(next_epoch_stake)
                        stakes.append(stake_amount)
                        total_stake += stake_amount
                    except (ValueError, TypeError):
                        pass
            
            if stakes:
                result.total_stake = total_stake
                result.voting_power_gini = calculate_gini_coefficient(stakes)
                
        except Exception as e:
            result.extraction_errors.append(f"graphql_validator_analysis_error: {str(e)}")