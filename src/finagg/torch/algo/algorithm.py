from tensordict import TensorDict

from ..specs import CompositeSpec, TensorSpec
from .batch import Batch


def init_buffer(
    num_envs: int,
    horizon: int,
    observation_spec: TensorSpec,
    feature_spec: TensorSpec,
    action_spec: TensorSpec,
    /,
) -> TensorDict:

    buffer_spec = CompositeSpec(
        **{
            Batch.OBS: observation_spec,
        }
    )
