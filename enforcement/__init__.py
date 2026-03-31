"""
enforcement/ — Codespaces Enforcement Framework for PC2 discovery artifacts.

Modules:
    artifact_validator   — JSON schema + domain constraint validation
    ownership_validator  — Ownership metadata consistency checks
    dependency_validator — Artifact phase-order dependency checks
    universal_analyzer   — Analyzer scaffolding (routing only at setup phase)
    gold_case_framework  — Gold-case manifest and stub emitter
    promotion_gate       — Promotion gate pre-condition checks
    report_loaders       — Typed loaders / parsers for PC2 artifact types

Schemas (enforcement/schemas/):
    business_viability_report.schema.json
    path_family_report.schema.json
    structure_truth.schema.json
    setup_truth.schema.json
    trigger_truth.schema.json
    ceiling_report.schema.json
    segmentation_gap_report.schema.json
    intervention_basis.schema.json
"""
