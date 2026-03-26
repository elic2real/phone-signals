#!/usr/bin/env python3
"""
Quarter Mapping Extractor
Extracts real quarter-specific configurations from research tune_map system
"""

import json
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import logging

from state_key import compute_session, compute_quarter

logger = logging.getLogger(__name__)

class QuarterMappingExtractor:
    """Extracts and caches quarter-specific configurations from research mappings"""
    
    def __init__(self):
        self._quarter_cache: Dict[str, Dict[str, Any]] = {}
        self._base_config: Dict[str, Any] = {}
        self._loaded = False
        
    def _load_tune_map_data(self) -> bool:
        """Load data from the research tune_map system"""
        try:
            # Load base tune map
            base_path = Path("tunes/tune_map_seed.json")
            if base_path.exists():
                with open(base_path, 'r') as f:
                    self._base_config = json.load(f)
                    
            # Load quarter-specific patches
            quarter_files = {
                "NY_Q1": "tunes/TUNE_MAP_NYQ1_CANDIDATE.json",
                "Q4_EXIT": "tunes/TUNE_MAP_Q4_EXIT_CANDIDATE.json"
            }
            
            for quarter_key, file_path in quarter_files.items():
                path = Path(file_path)
                if path.exists():
                    with open(path, 'r') as f:
                        data = json.load(f)
                        
                    # Extract patches
                    if "patches" in data:
                        for patch in data["patches"]:
                            if "key" in patch:
                                key = patch["key"]
                                pair = key.get("pair")
                                session = key.get("session")
                                quarter = key.get("quarter")
                                
                                if pair and session and quarter:
                                    cache_key = f"{pair}_{session.lower()}_{quarter}"
                                    self._quarter_cache[cache_key] = {
                                        "pair": pair,
                                        "session": session.lower(),
                                        "quarter": quarter,
                                        "entry_patch": patch.get("entry_patch", {}),
                                        "aee_patch": patch.get("aee_patch", {}),
                                        "evidence": patch.get("evidence", {}),
                                        "source": f"research_patch_{quarter_key}"
                                    }
                                    
            self._loaded = True
            logger.info(f"Loaded {len(self._quarter_cache)} quarter-specific patches")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load tune_map data: {e}")
            return False
            
    def get_quarter_config(self, pair: str, ts: float) -> Optional[Dict[str, Any]]:
        """Get quarter-specific configuration from research mapping"""
        if not self._loaded:
            if not self._load_tune_map_data():
                return None
                
        session = compute_session(ts).lower()
        quarter = compute_quarter(ts, compute_session(ts))
        
        # Try exact match
        cache_key = f"{pair}_{session}_{quarter}"
        if cache_key in self._quarter_cache:
            return self._prepare_config(self._quarter_cache[cache_key])
            
        # Try family match
        family = self._get_family(pair)
        if family:
            family_key = f"{family}_{session}_{quarter}"
            if family_key in self._quarter_cache:
                return self._prepare_config(self._quarter_cache[family_key])
                
        # Get base config if no quarter patch
        return self._get_base_config(pair, session, quarter)
        
    def _get_base_config(self, pair: str, session: str, quarter: str) -> Optional[Dict[str, Any]]:
        """Get base configuration from tune_map_seed"""
        if not self._base_config:
            return None
            
        # Look for session-pair config in base
        session_pair_key = f"session={session.upper()}|pair={pair}|speed=MED"
        
        # Check COARSE level first
        if "COARSE" in self._base_config and session_pair_key in self._base_config["COARSE"]:
            config = self._base_config["COARSE"][session_pair_key].copy()
            
            # Ensure required structure
            if "entry_filters" not in config:
                config["entry_filters"] = {}
            if "management" not in config:
                config["management"] = {}
            if "position_sizing" not in config:
                config["position_sizing"] = {}
            if "targets" not in config:
                config["targets"] = {}
                
            # Add metadata
            config["source"] = "research_base_coarse"
            config["pair"] = pair
            config["session"] = session
            config["quarter"] = quarter
            
            return config
            
        return None
        
    def _get_family(self, pair: str) -> Optional[str]:
        """Determine family for a pair"""
        if "JPY" in pair:
            return "JPY_FAMILY"
        elif "CHF" in pair:
            return "CHF_FAMILY"
        elif "USD" in pair:
            return "USD_FAMILY"
        return None
        
    def _prepare_config(self, cached_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare configuration for return"""
        # Start with base config if available
        config = {}
        
        # Get base config for this pair/session
        if self._base_config and "COARSE" in self._base_config:
            session_pair_key = f"session={cached_data['session'].upper()}|pair={cached_data['pair']}|speed=MED"
            if session_pair_key in self._base_config["COARSE"]:
                config = self._base_config["COARSE"][session_pair_key].copy()
                
        # Apply quarter patches
        if "aee_patch" in cached_data:
            config.update(cached_data["aee_patch"])
        if "entry_patch" in cached_data:
            config.update(cached_data["entry_patch"])
            
        # Ensure required structure
        if "entry_filters" not in config:
            config["entry_filters"] = {}
        if "management" not in config:
            config["management"] = {}
        if "position_sizing" not in config:
            config["position_sizing"] = {}
        if "targets" not in config:
            config["targets"] = {}
            
        # Add metadata
        config["source"] = cached_data.get("source", "research_mapping")
        config["pair"] = cached_data["pair"]
        config["session"] = cached_data["session"]
        config["quarter"] = cached_data["quarter"]
        
        if "evidence" in cached_data:
            config["evidence"] = cached_data["evidence"]
            
        return config
        
    def get_available_quarters(self, pair: str) -> Dict[str, list]:
        """Get available quarters for a pair"""
        if not self._loaded:
            if not self._load_tune_map_data():
                return {}
                
        result = {"ASIA": [], "LONDON": [], "NY": []}
        
        for key, data in self._quarter_cache.items():
            if data["pair"] == pair or data["pair"] == self._get_family(pair):
                session = data["session"].upper()
                quarter = data["quarter"]
                if quarter not in result[session]:
                    result[session].append(quarter)
                    
        return result
