#!/usr/bin/env python3
"""
Secrets Manager - Layer 0 Environment Integrity
Handle OANDA API credentials securely with validation
"""

import os
import sys
import json
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
from datetime import datetime

class SecretsError(Exception):
    """Secrets management failed"""
    pass

class SecretsValidationError(Exception):
    """Secrets validation failed"""
    pass

class SecretsManager:
    """Manage OANDA API credentials securely"""
    
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.secrets: Dict[str, Any] = {}
        self.validation_errors: list = []
        
        # No credential fallback defaults are allowed.
        self.default_credentials = {
            "OANDA_API_KEY": "",
            "OANDA_ACCOUNT_ID": "",
            "OANDA_ENV": "practice"
        }
    
    def load_secrets(self) -> Dict[str, Any]:
        """Load secrets from environment variables with fallback"""
        self.secrets = {}
        self.validation_errors.clear()
        
        # Try environment variables first
        env_api_key = os.getenv("OANDA_API_KEY")
        env_account_id = os.getenv("OANDA_ACCOUNT_ID")
        env_env = os.getenv("OANDA_ENV")
        
        # Use environment only; env defaults are empty except OANDA_ENV.
        self.secrets["OANDA_API_KEY"] = env_api_key or self.default_credentials["OANDA_API_KEY"]
        self.secrets["OANDA_ACCOUNT_ID"] = env_account_id or self.default_credentials["OANDA_ACCOUNT_ID"]
        self.secrets["OANDA_ENV"] = env_env or self.default_credentials["OANDA_ENV"]
        
        print(f"✅ Secrets loaded (env vars: {bool(env_api_key)})")
        return self.secrets
    
    def validate_secrets(self) -> bool:
        """Validate OANDA credentials format and structure"""
        self.validation_errors.clear()
        
        # Validate API Key format
        api_key = self.secrets.get("OANDA_API_KEY", "")
        if not api_key:
            self.validation_errors.append("OANDA_API_KEY is missing")
        elif len(api_key) < 20:
            self.validation_errors.append("OANDA_API_KEY appears too short")
        elif "-" not in api_key:
            self.validation_errors.append("OANDA_API_KEY format invalid (missing dash)")
        
        # Validate Account ID format
        account_id = self.secrets.get("OANDA_ACCOUNT_ID", "")
        if not account_id:
            self.validation_errors.append("OANDA_ACCOUNT_ID is missing")
        elif not isinstance(account_id, str):
            self.validation_errors.append("OANDA_ACCOUNT_ID must be a string")
        elif "-" not in account_id:
            self.validation_errors.append("OANDA_ACCOUNT_ID format invalid (missing dashes)")
        else:
            clean_id = account_id.replace("-", "")
            if not clean_id.isdigit():
                self.validation_errors.append("OANDA_ACCOUNT_ID should contain only digits and dashes")
        
        # Validate Environment
        env = self.secrets.get("OANDA_ENV", "")
        if not env:
            self.validation_errors.append("OANDA_ENV is missing")
        elif env not in ["practice", "live"]:
            self.validation_errors.append("OANDA_ENV must be 'practice' or 'live'")
        
        return len(self.validation_errors) == 0
    
    def redact_for_logging(self) -> Dict[str, str]:
        """Return redacted secrets for logging"""
        redacted = {}
        
        for key, value in self.secrets.items():
            if "API_KEY" in key:
                # Show first 8 and last 4 characters
                if len(value) > 12:
                    redacted[key] = f"{value[:8]}...{value[-4:]}"
                else:
                    redacted[key] = "***REDACTED***"
            elif "ACCOUNT_ID" in key:
                # Show first 3 and last 3 digits
                clean_id = value.replace("-", "")
                if len(clean_id) > 6:
                    redacted[key] = f"{clean_id[:3]}-{clean_id[-3:]}"
                else:
                    redacted[key] = "***REDACTED***"
            else:
                redacted[key] = value if key == "OANDA_ENV" else "***REDACTED***"
        
        return redacted
    
    def get_secrets_summary(self) -> Dict[str, Any]:
        """Get secrets summary for reporting (redacted)"""
        if not self.secrets:
            return {"error": "No secrets loaded"}
        
        return {
            "timestamp": datetime.now().isoformat(),
            "secrets_loaded": len(self.secrets),
            "validation_passed": len(self.validation_errors) == 0,
            "validation_errors": self.validation_errors,
            "redacted_secrets": self.redact_for_logging(),
            "source": "environment_variables"
        }
    
    def save_secrets_report(self, output_path: Optional[str] = None):
        """Save secrets validation report to JSON file"""
        if output_path is None:
            output_path = self.base_dir / "reports" / "layer0_secrets_validation.json"
        
        # Ensure reports directory exists
        output_path.parent.mkdir(exist_ok=True)
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "validation_passed": len(self.validation_errors) == 0,
            "validation_errors": self.validation_errors,
            "secrets_summary": self.get_secrets_summary()
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        
        print(f"📄 Secrets report saved: {output_path}")
    
    def test_connection_format(self) -> bool:
        """Test if credentials format looks valid for OANDA API"""
        try:
            api_key = self.secrets.get("OANDA_API_KEY", "")
            account_id = self.secrets.get("OANDA_ACCOUNT_ID", "")
            env = self.secrets.get("OANDA_ENV", "")
            
            # Basic format checks
            if not (api_key and account_id and env):
                return False
            
            # API key should be in format: xxxxxxxx-xxxxxxxx-xxxxxxxx-xxxxxxxx
            if len(api_key.split("-")) != 4:
                return False
            
            # Account ID should be in format: xxx-xxx-xxxxxxx-xxx
            if len(account_id.split("-")) != 4:
                return False
            
            return True
            
        except Exception:
            return False

def main():
    """Test secrets manager"""
    print("🔐 Testing Secrets Manager")
    print("=" * 50)
    
    try:
        manager = SecretsManager()
        
        # Load secrets
        secrets = manager.load_secrets()
        print(f"✅ Secrets loaded successfully")
        
        # Validate secrets
        if manager.validate_secrets():
            print("✅ Secrets validation passed")
        else:
            print("❌ Secrets validation failed:")
            for error in manager.validation_errors:
                print(f"  - {error}")
        
        # Test connection format
        if manager.test_connection_format():
            print("✅ Credentials format looks valid for OANDA API")
        else:
            print("⚠️  Credentials format may be invalid")
        
        # Show redacted secrets
        redacted = manager.redact_for_logging()
        print(f"\n🔒 Redacted Secrets:")
        for key, value in redacted.items():
            print(f"  {key}: {value}")
        
        # Show summary
        summary = manager.get_secrets_summary()
        print(f"\n📊 Secrets Summary:")
        print(f"  Source: {summary['source']}")
        print(f"  Validation: {'✅ PASSED' if summary['validation_passed'] else '❌ FAILED'}")
        
        # Save report
        manager.save_secrets_report()
        
        return len(manager.validation_errors) == 0
        
    except SecretsError as e:
        print(f"❌ Secrets error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
