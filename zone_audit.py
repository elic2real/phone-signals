"""Zone Classification Audit - Analyze why unified pipeline zones are classified as NOISE."""

import json
from typing import List
from unified_pipeline import UnifiedReverseEngineeringPipeline, BucketType, generate_sample_historical_data


def audit_zone_classification():
    """Audit why zones are classified as NOISE instead of GOOD/BAD."""

    print("=" * 80)
    print("ZONE CLASSIFICATION AUDIT")
    print("=" * 80)

    # Generate the same data used in unified pipeline
    historical_data = generate_sample_historical_data()

    # Create mapper to inspect classification logic
    pipeline = UnifiedReverseEngineeringPipeline()
    zones = pipeline.opportunity_mapper.map_opportunities(historical_data)

    print(f"\nAuditing {len(zones)} zones from unified pipeline")
    print()

    # Audit each zone
    for i, zone in enumerate(zones):
        print(f"ZONE {i+1}: {zone.zone_id}")
        print("-" * 40)

        # Basic zone info
        print(f"Direction: {zone.direction}")
        print(f"Bucket assigned: {zone.bucket}")
        print(f"Zone type: {zone.zone_type or 'None'}")

        # Key metrics
        print(f"MFE (R): {zone.mfe_r:.3f}")
        print(f"MAE (R): {zone.mae_r:.3f}")
        print(".3f")
        print(f"Tau hit: {zone.tau_hit} minutes")

        # Behavioral classifications
        print(f"Extension behavior: {zone.extension_behavior}")
        print(f"Stall behavior: {zone.stall_behavior}")
        print(f"Reversal behavior: {zone.reversal_behavior}")

        # Speed class
        print(f"Speed class: {zone.speed_class}")

        # Now audit WHY it's NOISE
        print("\nCLASSIFICATION ANALYSIS:")
        audit_noise_reason(zone)

        print()


def audit_noise_reason(zone):
    """Analyze why a zone was classified as NOISE."""

    # Get the features used for classification
    # We need to reconstruct what _classify_opportunity saw
    features = extract_zone_features_for_audit(zone)

    momentum = features["momentum"]
    volatility = features["volatility"]
    trend_strength = features["trend_strength"]
    energy_level = features["energy_level"]

    print(f"  Classification features: momentum={momentum:.3f}, volatility={volatility:.3f}, trend_strength={trend_strength:.3f}, energy_level={energy_level:.3f}")

    # Check GOOD criteria
    good_criteria = [
        ("momentum > 0.7", abs(momentum) > 0.7),
        ("volatility > 0.4", volatility > 0.4),
        ("trend_strength > 0.6", trend_strength > 0.6),
        ("energy_level > 0.6", energy_level > 0.6),
    ]

    print("  GOOD criteria check:")
    good_failed_criteria = []
    for desc, passed in good_criteria:
        status = "✅" if passed else "❌"
        print(f"    {status} {desc}")
        if not passed:
            good_failed_criteria.append(desc)

    # Check BAD criteria
    bad_criteria = [
        ("abs(momentum) < 0.3", abs(momentum) < 0.3),
        ("volatility > 0.6", volatility > 0.6),
        ("energy_level < 0.4", energy_level < 0.4),
    ]

    print("  BAD criteria check:")
    bad_passed_criteria = []
    for desc, passed in bad_criteria:
        status = "✅" if passed else "❌"
        print(f"    {status} {desc}")
        if passed:
            bad_passed_criteria.append(desc)

    # Determine why NOISE
    print("  WHY NOISE:")
    if len(good_failed_criteria) > 0:
        print(f"    ❌ Failed GOOD criteria: {', '.join(good_failed_criteria)}")
    else:
        print("    ✅ Passed all GOOD criteria - should be GOOD!")

    if len(bad_passed_criteria) == len(bad_criteria):
        print(f"    ❌ Also meets BAD criteria: {', '.join(bad_passed_criteria)}")
        print("    This zone might be both GOOD and BAD - classification conflict!")
    elif len(bad_passed_criteria) > 0:
        print(f"    ⚠️  Partially meets BAD criteria: {', '.join(bad_passed_criteria)}")

    if len(good_failed_criteria) > 0 and len(bad_passed_criteria) == 0:
        print("    → Clear NOISE: doesn't meet GOOD or BAD criteria")

    # Additional zone-specific analysis
    print("  ZONE-SPECIFIC ANALYSIS:")
    print(f"    MFE/MAE ratio: {zone.mfe_r/zone.mae_r:.3f} (higher is better)")
    print(f"    Net R: {zone.mfe_r - zone.mae_r:.3f} (positive is good)")

    # Check if zone has reasonable profit potential
    if zone.mfe_r > 1.0 and zone.mae_r < 0.5:
        print("    💡 Zone has good profit potential - might deserve GOOD classification")
    elif zone.mae_r > 1.0:
        print("    ⚠️  Zone has high risk - might deserve BAD classification")
    else:
        print("    🤔 Zone has moderate characteristics - NOISE classification reasonable")


def extract_zone_features_for_audit(zone):
    """Extract features that would be used for classification audit."""

    # Based on the zone data, reconstruct what classification features looked like
    # This is approximate since we don't have the original feature extraction

    # Momentum from direction and MFE
    momentum = zone.mfe_r if zone.direction == "LONG" else -zone.mfe_r

    # Volatility from ATR and speed class
    base_volatility = zone.atr_pips / 10.0  # Rough scaling
    speed_multiplier = {"fast": 1.2, "normal": 1.0, "slow": 0.8}.get(zone.speed_class, 1.0)
    volatility = base_volatility * speed_multiplier

    # Trend strength from extension behavior
    trend_strength_map = {
        "strong": 0.8,
        "moderate": 0.5,
        "weak": 0.2,
        "insufficient_data": 0.0
    }
    trend_strength = trend_strength_map.get(zone.extension_behavior, 0.5)

    # Energy level from reversal behavior (inverse relationship)
    energy_level_map = {
        "sharp": 0.2,  # Low energy if reverses sharply
        "gradual": 0.4,
        "false": 0.8,  # High energy if doesn't reverse
        "insufficient_data": 0.5
    }
    energy_level = energy_level_map.get(zone.reversal_behavior, 0.5)

    return {
        "momentum": momentum,
        "volatility": volatility,
        "trend_strength": trend_strength,
        "energy_level": energy_level,
    }


def analyze_classification_thresholds():
    """Analyze what thresholds would be needed to classify some zones as GOOD."""

    print("\n" + "=" * 80)
    print("THRESHOLD ANALYSIS")
    print("=" * 80)

    historical_data = generate_sample_historical_data()
    pipeline = UnifiedReverseEngineeringPipeline()
    zones = pipeline.opportunity_mapper.map_opportunities(historical_data)

    print(f"\nAnalyzing thresholds for {len(zones)} zones")

    # Collect all feature values
    all_features = []
    for zone in zones:
        features = extract_zone_features_for_audit(zone)
        features["bucket"] = zone.bucket
        features["direction"] = zone.direction
        all_features.append(features)

    # Analyze what thresholds would classify some as GOOD
    print("\nCURRENT GOOD THRESHOLDS:")
    print("  momentum > 0.7 (abs)")
    print("  volatility > 0.4")
    print("  trend_strength > 0.6")
    print("  energy_level > 0.6")

    print("\nFEATURE DISTRIBUTIONS:")
    for feature_name in ["momentum", "volatility", "trend_strength", "energy_level"]:
        values = [f[feature_name] for f in all_features]
        abs_values = [abs(v) if feature_name == "momentum" else v for v in values]

        print(f"  {feature_name}:")
        print(".3f")
        print(".3f")
        print(".3f")

        # Suggest relaxed thresholds
        if feature_name == "momentum":
            current_threshold = 0.7
            suggested = sorted(abs_values, reverse=True)[min(4, len(abs_values)-1)] if abs_values else 0
            print(".3f")
        elif feature_name == "volatility":
            current_threshold = 0.4
            suggested = sorted(values, reverse=True)[min(4, len(values)-1)] if values else 0
            print(".3f")
        elif feature_name == "trend_strength":
            current_threshold = 0.6
            suggested = sorted(values, reverse=True)[min(4, len(values)-1)] if values else 0
            print(".3f")
        elif feature_name == "energy_level":
            current_threshold = 0.6
            suggested = sorted(values, reverse=True)[min(4, len(values)-1)] if values else 0
            print(".3f")

    print("\nRECOMMENDATIONS:")
    print("1. Consider relaxing momentum threshold from 0.7 to ~0.5-0.6")
    print("2. Consider relaxing volatility threshold from 0.4 to ~0.3")
    print("3. Consider relaxing trend_strength threshold from 0.6 to ~0.4-0.5")
    print("4. Consider relaxing energy_level threshold from 0.6 to ~0.4")
    print("5. Test with sample data before applying to real market data")


if __name__ == "__main__":
    audit_zone_classification()
    analyze_classification_thresholds()
