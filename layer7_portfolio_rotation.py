#!/usr/bin/env python3
"""
Portfolio Rotation Layer - Layer 7 Production AEE Engine
Account multiplication and balanced mode optimization
"""

import sys
import os
import time
import math
import subprocess
import statistics
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
import json
from enum import Enum

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from layer6_aee_shadow import AEEStateMachine, AEEPosition, AEEState, AEEShadowManager

class AccountStatus(Enum):
    """Account status for rotation"""
    ACTIVE = "ACTIVE"
    COOLING = "COOLING"
    SUSPENDED = "SUSPENDED"
    RETIRED = "RETIRED"

@dataclass
class TradingAccount:
    """Trading account with rotation management"""
    account_id: str
    initial_balance: float
    current_balance: float
    status: AccountStatus
    performance_metrics: Dict[str, float] = field(default_factory=dict)
    rotation_history: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def calculate_return(self) -> float:
        """Calculate account return percentage"""
        if self.initial_balance <= 0:
            return 0.0
        return ((self.current_balance - self.initial_balance) / self.initial_balance) * 100
    
    def calculate_drawdown(self) -> float:
        """Calculate current drawdown percentage"""
        if not self.rotation_history:
            return 0.0
        
        peak_balance = max([h.get("balance_at_time", self.initial_balance) for h in self.rotation_history])
        if peak_balance <= 0:
            return 0.0
        
        current_drawdown = ((peak_balance - self.current_balance) / peak_balance) * 100
        return max(0.0, current_drawdown)
    
    def get_time_in_status(self) -> float:
        """Get time spent in current status"""
        current_time = time.time()
        
        # Find last status change
        for history_item in reversed(self.rotation_history):
            if "status_change" in history_item:
                return current_time - history_item["timestamp"]
        
        return current_time  # No previous status change

@dataclass
class PortfolioMetrics:
    """Portfolio-wide performance metrics"""
    total_accounts: int
    active_accounts: int
    total_balance: float
    total_return: float
    win_rate: float
    avg_r_multiple: float
    max_drawdown: float
    account_multiplication_factor: float
    distribution_stability: float
    balanced_mode_compliance: float
    timestamp: float
    metadata: Dict[str, Any] = field(default_factory=dict)

class BalancedModeOptimizer:
    """Balanced mode optimization for 63-68% WR and 1:1 R targets"""
    
    def __init__(self):
        # Target metrics
        self.target_win_rate = 0.65  # 65% target win rate (63-68% range)
        self.target_r_multiple = 1.0  # 1:1 R multiple target
        self.max_drawdown_threshold = 10.0  # 10% maximum drawdown
        
        # Optimization parameters
        self.min_trades_for_stats = 20  # Minimum trades for reliable statistics
        self.rebalance_threshold = 5.0  # 5% deviation triggers rebalance
        self.rotation_interval_days = 30  # 30 days between rotations
    
    def calculate_balanced_score(self, win_rate: float, avg_r_multiple: float, drawdown: float) -> float:
        """Calculate balanced mode compliance score"""
        # Win rate score (63-68% range)
        if 0.63 <= win_rate <= 0.68:
            win_rate_score = 1.0
        elif win_rate < 0.63:
            win_rate_score = win_rate / 0.63
        else:  # win_rate > 0.68
            win_rate_score = max(0, 1.0 - (win_rate - 0.68) / 0.32)
        
        # R multiple score (around 1.0)
        r_multiple_score = 1.0 - abs(avg_r_multiple - 1.0)
        r_multiple_score = max(0, r_multiple_score)
        
        # Drawdown score (lower is better)
        drawdown_score = max(0, 1.0 - (drawdown / self.max_drawdown_threshold))
        
        # Weighted average
        balanced_score = (win_rate_score * 0.4 + r_multiple_score * 0.3 + drawdown_score * 0.3)
        
        return balanced_score
    
    def should_rebalance(self, metrics: PortfolioMetrics) -> Tuple[bool, str]:
        """Check if portfolio should be rebalanced"""
        reasons = []
        
        # Check win rate deviation
        if abs(metrics.win_rate - self.target_win_rate) > 0.05:  # 5% deviation
            reasons.append(f"Win rate deviation: {metrics.win_rate:.1%} vs target {self.target_win_rate:.1%}")
        
        # Check R multiple deviation
        if abs(metrics.avg_r_multiple - self.target_r_multiple) > 0.2:  # 0.2R deviation
            reasons.append(f"R multiple deviation: {metrics.avg_r_multiple:.2f} vs target {self.target_r_multiple:.2f}")
        
        # Check drawdown
        if metrics.max_drawdown > self.max_drawdown_threshold:
            reasons.append(f"Drawdown exceeded: {metrics.max_drawdown:.1f}% vs threshold {self.max_drawdown_threshold:.1f}%")
        
        should_rebalance = len(reasons) > 0
        rebalance_reason = "; ".join(reasons) if reasons else ""
        
        return should_rebalance, rebalance_reason

class AccountRotationManager:
    """Manage account rotation and multiplication"""
    
    def __init__(self):
        self.accounts: Dict[str, TradingAccount] = {}
        self.rotation_queue: List[str] = []
        self.balanced_optimizer = BalancedModeOptimizer()
        
        # Rotation parameters
        self.max_concurrent_accounts = 5
        self.min_account_balance = 1000.0  # Minimum balance for active trading
        self.rotation_cooldown_days = 7  # Days between rotations
        self.multiplication_threshold = 1.2  # 20% profit triggers multiplication
        
        # Performance tracking
        self.total_trades = 0
        self.winning_trades = 0
        self.total_r_multiple = 0.0
        self.peak_balance = 0.0
        self.current_drawdown = 0.0
    
    def add_account(self, account_id: str, initial_balance: float) -> TradingAccount:
        """Add new trading account"""
        account = TradingAccount(
            account_id=account_id,
            initial_balance=initial_balance,
            current_balance=initial_balance,
            status=AccountStatus.ACTIVE,
            rotation_history=[{
                "timestamp": time.time(),
                "action": "account_created",
                "balance_at_time": initial_balance,
                "status": AccountStatus.ACTIVE.value
            }]
        )
        
        self.accounts[account_id] = account
        self.rotation_queue.append(account_id)
        
        return account
    
    def update_account_performance(self, account_id: str, pnl: float, is_win: bool, r_multiple: float):
        """Update account performance metrics"""
        if account_id not in self.accounts:
            return
        
        account = self.accounts[account_id]
        account.current_balance += pnl
        
        # Update performance metrics
        metrics = account.performance_metrics
        metrics["total_trades"] = metrics.get("total_trades", 0) + 1
        metrics["winning_trades"] = metrics.get("winning_trades", 0) + (1 if is_win else 0)
        metrics["total_r_multiple"] = metrics.get("total_r_multiple", 0) + r_multiple
        metrics["avg_r_multiple"] = metrics["total_r_multiple"] / metrics["total_trades"]
        metrics["win_rate"] = metrics["winning_trades"] / metrics["total_trades"]
        
        # Update rotation history
        account.rotation_history.append({
            "timestamp": time.time(),
            "action": "trade_completed",
            "balance_at_time": account.current_balance,
            "pnl": pnl,
            "is_win": is_win,
            "r_multiple": r_multiple
        })
        
        # Keep only last 100 history items
        if len(account.rotation_history) > 100:
            account.rotation_history = account.rotation_history[-100:]
        
        # Update portfolio metrics
        self._update_portfolio_metrics()
        
        # Check for account multiplication
        if account.calculate_return() >= self.multiplication_threshold * 100:
            self._multiply_account(account_id)
    
    def _multiply_account(self, account_id: str):
        """Multiply account when profit threshold is reached"""
        if account_id not in self.accounts:
            return
        
        account = self.accounts[account_id]
        
        # Create new account with multiplied balance
        new_balance = account.current_balance * 2  # Double the balance
        new_account_id = f"{account_id}_MULTIPLIED_{int(time.time())}"
        
        new_account = self.add_account(new_account_id, new_balance)
        
        # Update original account status
        account.status = AccountStatus.RETIRED
        account.rotation_history.append({
            "timestamp": time.time(),
            "action": "account_multiplied",
            "balance_at_time": account.current_balance,
            "new_account_id": new_account_id
        })
        
        print(f"💰 Account Multiplied: {account_id} -> {new_account_id}")
        print(f"   Old Balance: ${account.current_balance:.2f}")
        print(f"   New Balance: ${new_balance:.2f}")
    
    def rotate_accounts(self) -> List[str]:
        """Rotate accounts based on performance and status"""
        rotated_accounts = []
        current_time = time.time()
        
        # Check each account for rotation eligibility
        for account_id, account in self.accounts.items():
            should_rotate = False
            rotation_reason = ""
            
            # Check if account is cooling and can be reactivated
            if account.status == AccountStatus.COOLING:
                time_in_status = account.get_time_in_status()
                if time_in_status > (self.rotation_cooldown_days * 24 * 3600):  # Convert days to seconds
                    should_rotate = True
                    rotation_reason = "Cooling period completed"
            
            # Check if account is underperforming
            elif account.status == AccountStatus.ACTIVE:
                drawdown = account.calculate_drawdown()
                if drawdown > 15.0:  # 15% drawdown triggers cooling
                    should_rotate = True
                    rotation_reason = f"High drawdown: {drawdown:.1f}%"
                
                # Check for consistent losses
                metrics = account.performance_metrics
                if metrics.get("total_trades", 0) >= 10:
                    win_rate = metrics.get("win_rate", 0)
                    if win_rate < 0.4:  # Below 40% win rate
                        should_rotate = True
                        rotation_reason = f"Low win rate: {win_rate:.1%}"
            
            # Perform rotation if needed
            if should_rotate:
                old_status = account.status
                account.status = AccountStatus.COOLING if old_status == AccountStatus.ACTIVE else AccountStatus.ACTIVE
                
                account.rotation_history.append({
                    "timestamp": current_time,
                    "action": "account_rotated",
                    "old_status": old_status.value,
                    "new_status": account.status.value,
                    "balance_at_time": account.current_balance,
                    "reason": rotation_reason
                })
                
                rotated_accounts.append(account_id)
                print(f"🔄 Account Rotated: {account_id}")
                print(f"   {old_status.value} -> {account.status.value}")
                print(f"   Reason: {rotation_reason}")
        
        return rotated_accounts
    
    def _update_portfolio_metrics(self):
        """Update portfolio-wide performance metrics"""
        self.total_trades = sum(acc.performance_metrics.get("total_trades", 0) for acc in self.accounts.values())
        self.winning_trades = sum(acc.performance_metrics.get("winning_trades", 0) for acc in self.accounts.values())
        self.total_r_multiple = sum(acc.performance_metrics.get("total_r_multiple", 0) for acc in self.accounts.values())
        
        # Calculate current drawdown
        total_balance = sum(acc.current_balance for acc in self.accounts.values())
        if total_balance > self.peak_balance:
            self.peak_balance = total_balance
        
        if self.peak_balance > 0:
            self.current_drawdown = ((self.peak_balance - total_balance) / self.peak_balance) * 100
        else:
            self.current_drawdown = 0.0
    
    def get_portfolio_metrics(self) -> PortfolioMetrics:
        """Get comprehensive portfolio metrics"""
        active_accounts = [acc for acc in self.accounts.values() if acc.status == AccountStatus.ACTIVE]
        
        # Calculate aggregate metrics
        total_balance = sum(acc.current_balance for acc in active_accounts)
        total_initial = sum(acc.initial_balance for acc in active_accounts)
        total_return = ((total_balance - total_initial) / total_initial * 100) if total_initial > 0 else 0
        
        # Calculate win rate and R multiple
        total_trades = sum(acc.performance_metrics.get("total_trades", 0) for acc in active_accounts)
        winning_trades = sum(acc.performance_metrics.get("winning_trades", 0) for acc in active_accounts)
        win_rate = winning_trades / max(1, total_trades)
        
        total_r_multiple = sum(acc.performance_metrics.get("total_r_multiple", 0) for acc in active_accounts)
        avg_r_multiple = total_r_multiple / max(1, total_trades)
        
        # Calculate max drawdown
        max_drawdown = max(acc.calculate_drawdown() for acc in self.accounts.values())
        
        # Calculate account multiplication factor
        retired_accounts = [acc for acc in self.accounts.values() if acc.status == AccountStatus.RETIRED]
        multiplication_factor = len(retired_accounts) / max(1, len(self.accounts))
        
        # Calculate distribution stability
        balance_variance = statistics.variance([acc.current_balance for acc in active_accounts]) if active_accounts else 0
        balance_mean = statistics.mean([acc.current_balance for acc in active_accounts]) if active_accounts else 1
        distribution_stability = 1.0 - (balance_variance / (balance_mean ** 2)) if balance_mean > 0 else 0
        
        # Calculate balanced mode compliance
        balanced_score = self.balanced_optimizer.calculate_balanced_score(win_rate, avg_r_multiple, max_drawdown)
        
        return PortfolioMetrics(
            total_accounts=len(self.accounts),
            active_accounts=len(active_accounts),
            total_balance=total_balance,
            total_return=total_return,
            win_rate=win_rate,
            avg_r_multiple=avg_r_multiple,
            max_drawdown=max_drawdown,
            account_multiplication_factor=multiplication_factor,
            distribution_stability=distribution_stability,
            balanced_mode_compliance=balanced_score,
            timestamp=time.time(),
            metadata={
                "retired_accounts": len(retired_accounts),
                "cooling_accounts": len([acc for acc in self.accounts.values() if acc.status == AccountStatus.COOLING]),
                "suspended_accounts": len([acc for acc in self.accounts.values() if acc.status == AccountStatus.SUSPENDED])
            }
        )

class PortfolioRotationManager:
    """Main portfolio rotation manager"""
    
    def __init__(self, aee_manager: AEEShadowManager):
        self.aee_manager = aee_manager
        self.rotation_manager = AccountRotationManager()
        self.balanced_optimizer = BalancedModeOptimizer()
        
        # Portfolio configuration - will be updated with real balance from OANDA
        self.initial_account_balance = 0.0  # Will be set from real account data
        self.max_accounts = 10
        self.rebalance_interval_hours = 24  # Check daily
        
        # Performance tracking
        self.last_rebalance_time = 0.0
        self.rebalance_history: List[Dict[str, Any]] = []
    
    def set_real_account_balance(self, real_balance: float):
        """Set the real account balance from OANDA API"""
        self.initial_account_balance = real_balance
        print(f"💰 Real account balance set: ${real_balance:.2f}")
    
    def initialize_portfolio(self, num_accounts: int = 3, real_balance: float = None) -> List[TradingAccount]:
        """Initialize portfolio with starting accounts"""
        if real_balance is not None:
            self.set_real_account_balance(real_balance)
        elif self.initial_account_balance == 0.0:
            raise ValueError("Account balance not set. Call set_real_account_balance() or pass real_balance parameter")
        
        accounts = []
        
        for i in range(num_accounts):
            account_id = f"ACCOUNT_{i+1:03d}"
            account = self.rotation_manager.add_account(account_id, self.initial_account_balance)
            accounts.append(account)
        
        print(f"🏦 Portfolio Initialized: {num_accounts} accounts")
        print(f"   Total Balance: ${num_accounts * self.initial_account_balance:.2f}")
        
        return accounts
    
    def process_aee_results(self, aee_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process AEE results and update portfolio"""
        updates = {
            "accounts_updated": 0,
            "accounts_rotated": 0,
            "accounts_multiplied": 0,
            "portfolio_metrics": None
        }
        
        # Process each AEE result
        for result in aee_results:
            account_id = result.get("account_id")
            pnl = result.get("pnl", 0.0)
            is_win = result.get("is_win", False)
            r_multiple = result.get("r_multiple", 0.0)
            
            if account_id:
                self.rotation_manager.update_account_performance(account_id, pnl, is_win, r_multiple)
                updates["accounts_updated"] += 1
        
        # Check for account rotation
        rotated_accounts = self.rotation_manager.rotate_accounts()
        updates["accounts_rotated"] = len(rotated_accounts)
        
        # Get portfolio metrics
        updates["portfolio_metrics"] = self.rotation_manager.get_portfolio_metrics()
        
        return updates
    
    def check_rebalance_needed(self) -> Tuple[bool, str]:
        """Check if portfolio rebalancing is needed"""
        current_time = time.time()
        
        # Check time-based rebalance
        if current_time - self.last_rebalance_time > (self.rebalance_interval_hours * 3600):
            return True, "Scheduled rebalance"
        
        # Check performance-based rebalance
        metrics = self.rotation_manager.get_portfolio_metrics()
        should_rebalance, reason = self.balanced_optimizer.should_rebalance(metrics)
        
        return should_rebalance, reason
    
    def rebalance_portfolio(self) -> Dict[str, Any]:
        """Rebalance portfolio based on performance"""
        current_time = time.time()
        
        # Get current metrics
        metrics = self.rotation_manager.get_portfolio_metrics()
        
        # Determine rebalance actions
        rebalance_actions = []
        
        # Check for underperforming accounts
        for account_id, account in self.rotation_manager.accounts.items():
            if account.status == AccountStatus.ACTIVE:
                drawdown = account.calculate_drawdown()
                if drawdown > 15.0:
                    rebalance_actions.append({
                        "account_id": account_id,
                        "action": "cooling",
                        "reason": f"High drawdown: {drawdown:.1f}%"
                    })
        
        # Check for new account opportunities
        if len([acc for acc in self.rotation_manager.accounts.values() if acc.status == AccountStatus.ACTIVE]) < 3:
            rebalance_actions.append({
                "account_id": f"NEW_ACCOUNT_{int(current_time)}",
                "action": "create",
                "reason": "Insufficient active accounts"
            })
        
        # Record rebalance
        rebalance_record = {
            "timestamp": current_time,
            "metrics_before": metrics,
            "actions_taken": rebalance_actions,
            "reason": "Portfolio rebalance"
        }
        
        self.rebalance_history.append(rebalance_record)
        self.last_rebalance_time = current_time
        
        # Keep only last 50 rebalance records
        if len(self.rebalance_history) > 50:
            self.rebalance_history = self.rebalance_history[-50:]
        
        return {
            "rebalance_completed": True,
            "actions_taken": len(rebalance_actions),
            "metrics_after": self.rotation_manager.get_portfolio_metrics(),
            "rebalance_record": rebalance_record
        }
    
    def get_portfolio_summary(self) -> Dict[str, Any]:
        """Get comprehensive portfolio summary"""
        metrics = self.rotation_manager.get_portfolio_metrics()
        
        summary = {
            "timestamp": datetime.now().isoformat(),
            "portfolio_metrics": {
                "total_accounts": metrics.total_accounts,
                "active_accounts": metrics.active_accounts,
                "total_balance": metrics.total_balance,
                "total_return": metrics.total_return,
                "win_rate": metrics.win_rate,
                "avg_r_multiple": metrics.avg_r_multiple,
                "max_drawdown": metrics.max_drawdown,
                "account_multiplication_factor": metrics.account_multiplication_factor,
                "distribution_stability": metrics.distribution_stability,
                "balanced_mode_compliance": metrics.balanced_mode_compliance,
                "timestamp": metrics.timestamp
            },
            "account_status": {
                "total": metrics.total_accounts,
                "active": metrics.active_accounts,
                "retired": metrics.metadata.get("retired_accounts", 0),
                "cooling": metrics.metadata.get("cooling_accounts", 0),
                "suspended": metrics.metadata.get("suspended_accounts", 0)
            },
            "performance": {
                "total_balance": metrics.total_balance,
                "total_return": metrics.total_return,
                "win_rate": metrics.win_rate,
                "avg_r_multiple": metrics.avg_r_multiple,
                "max_drawdown": metrics.max_drawdown
            },
            "balanced_mode": {
                "compliance_score": metrics.balanced_mode_compliance,
                "target_win_rate": self.balanced_optimizer.target_win_rate,
                "target_r_multiple": self.balanced_optimizer.target_r_multiple,
                "max_drawdown_threshold": self.balanced_optimizer.max_drawdown_threshold
            },
            "account_multiplication": {
                "multiplication_factor": metrics.account_multiplication_factor,
                "threshold": self.rotation_manager.multiplication_threshold,
                "total_multiplied": metrics.metadata.get("retired_accounts", 0)
            },
            "distribution": {
                "stability_score": metrics.distribution_stability,
                "active_accounts": len([acc for acc in self.rotation_manager.accounts.values() if acc.status == AccountStatus.ACTIVE])
            }
        }
        
        return summary

def main():
    """Test portfolio rotation layer"""
    print("🏦 LAYER 7 - PORTFOLIO ROTATION LAYER")
    print("=" * 50)
    
    try:
        # Initialize components
        aee_manager = AEEShadowManager(None, None)
        portfolio_manager = PortfolioRotationManager(aee_manager)
        
        print("🔬 Testing portfolio rotation...")
        
        # Initialize portfolio
        accounts = portfolio_manager.initialize_portfolio(3)
        
        # Simulate AEE results over time
        simulation_results = []
        
        for day in range(30):  # 30 days simulation
            daily_results = []
            
            # Simulate trades for each active account
            for account in accounts:
                if account.status == AccountStatus.ACTIVE:
                    # Simulate trade results (70% win rate, 1:1 R multiple)
                    is_win = day % 10 < 7  # 70% win rate
                    r_multiple = 1.0 if is_win else -1.0
                    pnl = account.initial_balance * 0.02 * r_multiple  # 2% risk per trade
                    
                    daily_results.append({
                        "account_id": account.account_id,
                        "pnl": pnl,
                        "is_win": is_win,
                        "r_multiple": r_multiple
                    })
            
            # Process results
            updates = portfolio_manager.process_aee_results(daily_results)
            simulation_results.append(updates)
            
            # Check for rebalancing
            should_rebalance, reason = portfolio_manager.check_rebalance_needed()
            
            if should_rebalance:
                rebalance = portfolio_manager.rebalance_portfolio()
                print(f"Day {day+1}: Rebalance - {reason}")
                print(f"  Actions: {rebalance['actions_taken']}")
            
            # Show weekly summary
            if (day + 1) % 7 == 0:
                summary = portfolio_manager.get_portfolio_summary()
                print(f"\n--- Week {((day + 1) // 7)} Summary ---")
                print(f"  Total Balance: ${summary['performance']['total_balance']:.2f}")
                print(f"  Total Return: {summary['performance']['total_return']:.1f}%")
                print(f"  Win Rate: {summary['performance']['win_rate']:.1%}")
                print(f"  Active Accounts: {summary['account_status']['active']}")
                print(f"  Multiplied Accounts: {summary['account_multiplication']['total_multiplied']}")
        
        # Get final portfolio summary
        final_summary = portfolio_manager.get_portfolio_summary()
        
        print(f"\n📊 Final Portfolio Summary:")
        print(f"  Total Balance: ${final_summary['performance']['total_balance']:.2f}")
        print(f"  Total Return: {final_summary['performance']['total_return']:.1f}%")
        print(f"  Win Rate: {final_summary['performance']['win_rate']:.1%}")
        print(f"  Avg R Multiple: {final_summary['performance']['avg_r_multiple']:.2f}")
        print(f"  Max Drawdown: {final_summary['performance']['max_drawdown']:.1f}%")
        print(f"  Balanced Mode Score: {final_summary['balanced_mode']['compliance_score']:.2f}")
        print(f"  Account Multiplication: {final_summary['account_multiplication']['multiplication_factor']:.2f}x")
        print(f"  Distribution Stability: {final_summary['distribution']['stability_score']:.2f}")
        
        # Save results
        # Convert simulation_results to JSON serializable format
        serializable_results = []
        for result in simulation_results:
            serializable_result = {
                "accounts_updated": result.get("accounts_updated", 0),
                "accounts_rotated": result.get("accounts_rotated", 0),
                "accounts_multiplied": result.get("accounts_multiplied", 0),
                "portfolio_metrics": {
                    "total_accounts": result.get("portfolio_metrics", {}).get("total_accounts", 0),
                    "active_accounts": result.get("portfolio_metrics", {}).get("active_accounts", 0),
                    "total_balance": result.get("portfolio_metrics", {}).get("total_balance", 0),
                    "total_return": result.get("portfolio_metrics", {}).get("total_return", 0),
                    "win_rate": result.get("portfolio_metrics", {}).get("win_rate", 0),
                    "avg_r_multiple": result.get("portfolio_metrics", {}).get("avg_r_multiple", 0),
                    "max_drawdown": result.get("portfolio_metrics", {}).get("max_drawdown", 0),
                    "account_multiplication_factor": result.get("portfolio_metrics", {}).get("account_multiplication_factor", 0),
                    "distribution_stability": result.get("portfolio_metrics", {}).get("distribution_stability", 0),
                    "balanced_mode_compliance": result.get("portfolio_metrics", {}).get("balanced_mode_compliance", 0),
                    "timestamp": result.get("portfolio_metrics", {}).get("timestamp", 0)
                }
            }
            serializable_results.append(serializable_result)
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "layer": 7,
            "test_name": "portfolio_rotation",
            "simulation_days": 30,
            "initial_accounts": len(accounts),
            "final_summary": {
                "performance": {
                    "total_balance": final_summary['performance']['total_balance'],
                    "total_return": final_summary['performance']['total_return'],
                    "win_rate": final_summary['performance']['win_rate'],
                    "avg_r_multiple": final_summary['performance']['avg_r_multiple'],
                    "max_drawdown": final_summary['performance']['max_drawdown']
                },
                "balanced_mode": {
                    "compliance_score": final_summary['balanced_mode']['compliance_score'],
                    "target_win_rate": final_summary['balanced_mode']['target_win_rate'],
                    "target_r_multiple": final_summary['balanced_mode']['target_r_multiple'],
                    "max_drawdown_threshold": final_summary['balanced_mode']['max_drawdown_threshold']
                },
                "account_multiplication": {
                    "multiplication_factor": final_summary['account_multiplication']['multiplication_factor'],
                    "threshold": final_summary['account_multiplication']['threshold'],
                    "total_multiplied": final_summary['account_multiplication']['total_multiplied']
                },
                "distribution": {
                    "stability_score": final_summary['distribution']['stability_score'],
                    "active_accounts": final_summary['distribution']['active_accounts']
                }
            },
            "simulation_results": serializable_results,
            "rebalance_history": portfolio_manager.rebalance_history,
            "balanced_mode_targets": {
                "win_rate_target": portfolio_manager.balanced_optimizer.target_win_rate,
                "r_multiple_target": portfolio_manager.balanced_optimizer.target_r_multiple,
                "max_drawdown_threshold": portfolio_manager.balanced_optimizer.max_drawdown_threshold
            }
        }
        
        reports_dir = Path(__file__).parent / "reports"
        reports_dir.mkdir(exist_ok=True)
        
        report_path = reports_dir / "layer7_portfolio_rotation_test.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n📄 Test report saved: {report_path}")
        
        # Validate requirements
        success = (
            final_summary['performance']['total_return'] > 0 and
            final_summary['balanced_mode']['compliance_score'] > 0.6 and
            final_summary['performance']['max_drawdown'] < 15.0 and
            final_summary['account_multiplication']['multiplication_factor'] >= 0
        )
        
        print(f"\n{'✅ SUCCESS' if success else '❌ FAILED'}: Portfolio rotation validation")
        
        return success
        
    except Exception as e:
        print(f"❌ Portfolio rotation error: {e}")
        return False

if __name__ == "__main__":
    import shutil
    success = main()
    sys.exit(0 if success else 1)
