from pydantic import BaseModel


class AlgorithmConfig(BaseModel):
    env: "EnvConfig"

    policy: "PolicyConfig"

    learning: "LearningConfig"

    evaluation: "EvaluationConfig"

    checkpoint: "CheckpointConfig"

    logger: "LoggerConfig"


class CheckpointConfig(BaseModel):
    ...


class EnvConfig(BaseModel):
    ...


class EvaluationConfig(BaseModel):
    ...


class LearningConfig(BaseModel):
    ...


class LoggerConfig(BaseModel):
    ...


class PolicyConfig(BaseModel):
    ...
