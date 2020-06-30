from typing import Optional
from uuid import uuid4

from hpc.autoscale import hpctypes as ht


class DelayedNodeId:
    """
        CycleCloud 7.9 does not support providing the NodeId in the
        creation call, so unfortunately the node id is not set until
        after the node is created. This class maintains the following ids

        name: the name of the node.
        operation_id: the most recent operation id associated with the node
                            (creation / deallocation / deletion)
        transient_id: a unique id for this node during this instance of the
                        node manager.
        and finally,
        node_id: the NodeId value from CycleCloud.

        Logging this should capture enough information to track the lifecycle of the
        node from internal allocation to external creation. Note that if the node
        already exists, the node_id will always be present.

        NOTE: In future releases of CycleCloud, this will not be required and we
        should be able to have one unique NodeId.
    """

    def __init__(
        self,
        name: ht.NodeName,
        node_id: ht.NodeId = None,
        operation_id: Optional[ht.OperationId] = None,
        operation_offset: Optional[int] = None,
    ) -> None:
        self.name = name
        self.transient_id = ht.NodeId(str(uuid4()))
        self.operation_id = operation_id
        self.operation_offset = operation_offset
        self.node_id = node_id

    def __str__(self) -> ht.NodeId:
        return ht.NodeId(
            "NodeId(name={}, node_id={}, operation_id={}, operation_offset={}, transient_id={})".format(
                self.name,
                self.node_id,
                self.operation_id,
                self.operation_offset,
                self.transient_id,
            )
        )

    def __repr__(self) -> str:
        return str(self)

    def clone(self) -> "DelayedNodeId":
        return DelayedNodeId(
            self.name, self.node_id, self.operation_id, self.operation_offset
        )

    def to_json(self) -> str:
        return self.node_id or ""
