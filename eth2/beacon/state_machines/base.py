from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Tuple,
    Type,
)

from eth_typing import (
    Hash32,
)

from eth._utils.datatypes import (
    Configurable,
)

from eth2.beacon.db.chain import BaseBeaconChainDB
from eth2.beacon.types.blocks import BaseBeaconBlock
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import (
    FromBlockParams,
)

from .state_transitions import (
    BaseStateTransition,
)
from .configs import (  # noqa: F401
    BeaconConfig,
)


class BaseBeaconStateMachine(Configurable, ABC):
    fork = None  # type: str
    chaindb = None  # type: BaseBeaconChainDB
    config = None  # type: BeaconConfig

    block = None  # type: BaseBeaconBlock
    _state = None  # type: BeaconState

    block_class = None  # type: Type[BaseBeaconBlock]
    state_class = None  # type: Type[BeaconState]
    state_transition_class = None  # type: Type[BaseStateTransition]

    @abstractmethod
    def __init__(self,
                 chaindb: BaseBeaconChainDB,
                 block_root: Hash32) -> None:
        pass

    @classmethod
    @abstractmethod
    def get_block_class(cls) -> Type[BaseBeaconBlock]:
        pass

    @classmethod
    @abstractmethod
    def get_state_class(cls) -> Type[BeaconState]:
        pass

    @classmethod
    @abstractmethod
    def get_state_transiton_class(cls) -> Type[BaseStateTransition]:
        pass

    @property
    @abstractmethod
    def state_transition(self) -> BaseStateTransition:
        pass

    #
    # Import block API
    #
    @abstractmethod
    def import_block(self,
                     block: BaseBeaconBlock,
                     check_proposer_signature: bool=False) -> Tuple[BeaconState, BaseBeaconBlock]:
        pass

    @staticmethod
    @abstractmethod
    def create_block_from_parent(parent_block: BaseBeaconBlock,
                                 block_params: FromBlockParams) -> BaseBeaconBlock:
        pass


class BeaconStateMachine(BaseBeaconStateMachine):
    def __init__(self,
                 chaindb: BaseBeaconChainDB,
                 block: BaseBeaconBlock) -> None:
        self.chaindb = chaindb
        self.block = block

    @property
    def state(self) -> BeaconState:
        if self._state is None:
            self._state = self.chaindb.get_state_by_root(self.block.state_root)
        return self._state

    @classmethod
    def get_block_class(cls) -> Type[BaseBeaconBlock]:
        """
        Return the :class:`~eth2.beacon.types.blocks.BeaconBlock` class that this
        StateMachine uses for blocks.
        """
        if cls.block_class is None:
            raise AttributeError("No `block_class` has been set for this StateMachine")
        else:
            return cls.block_class

    @classmethod
    def get_state_class(cls) -> Type[BeaconState]:
        """
        Return the :class:`~eth2.beacon.types.states.BeaconState` class that this
        StateMachine uses for BeaconState.
        """
        if cls.state_class is None:
            raise AttributeError("No `state_class` has been set for this StateMachine")
        else:
            return cls.state_class

    @classmethod
    def get_state_transiton_class(cls) -> Type[BaseStateTransition]:
        """
        Return the :class:`~eth2.beacon.state_machines.state_transitions.BaseStateTransition`
        class that this StateTransition uses for StateTransition.
        """
        if cls.state_transition_class is None:
            raise AttributeError("No `state_transition_class` has been set for this StateMachine")
        else:
            return cls.state_transition_class

    @property
    def state_transition(self) -> BaseStateTransition:
        return self.get_state_transiton_class()(self.config)

    #
    # Import block API
    #
    def import_block(self,
                     block: BaseBeaconBlock,
                     check_proposer_signature: bool=False) -> Tuple[BeaconState, BaseBeaconBlock]:
        state = self.state_transition.apply_state_transition(
            self.state,
            block,
            check_proposer_signature,
        )

        block = block.copy(
            state_root=state.root,
        )

        return state, block
