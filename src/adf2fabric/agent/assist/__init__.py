"""Model-assisted drafts for pipelines the deterministic converter cannot fully convert.

Optional (``ADF2FABRIC_LLM=on``), per pipeline and on request: a person asks for a draft, reviews it next to the source and what the converter did,
and accepts it. The draft is corrected ADF *source*, never Fabric JSON: the deterministic converter still produces every Fabric file, and only an
accepted draft whose source has not changed since is used, only by ``convert --assisted``, and always marked as assisted.
"""
