
from dataclasses import dataclass

@dataclass
class KVConnectorOutput:
    # [req_ids]
    finished_sending: set[str] | None = None
    finished_recving: set[str] | None = None



    # Configuration describing how many finished sending/receiving
    # notifications should be expected for each request. This allows
    # handshake-based connectors like Nixl to update the KVOutputAggregator.
    # It captures a static setup info and should almost always remain constant
    # for a given connector after discovery. Default value entails no change.
    expected_finished_count: int = 0

    def is_empty(self):
        return (
            not self.finished_sending
            and not self.finished_recving
            and not self.kv_connector_stats
            and not self.kv_cache_events
            and not self.invalid_block_ids
        )



class MORIIO_KV_CONNECTOR:
    def __init__(self, config=None):
        self.config = config
        self.connection = None
        
    def get_num_new_matched_tokens(self,seq):
        # Placeholder implementation
        return 0, True  
    
    
class KVOutputAggregator:
    """Utility class to aggregate the output of all workers into a single
    output corresponding to Rank 0 for scheduler."""

    def __init__(self, expected_finished_count: int):
        # Complete transfer tracker. Used to track finished requests
        # [req_id -> n_remaining_workers]
        self._recv_remaining_count = dict[str, int]()
        self._send_remaining_count = dict[str, int]()
        self._expected_finished_count = expected_finished_count

    @classmethod
    def from_connector(cls, connector, world_size: int):
        return cls(connector.get_finished_count() or world_size)

    def aggregate(
        self, outputs: list, output_rank: int = 0
    ) :
        if not outputs[output_rank]:
            return None
        # Aggregate kv_connector_output from all workers
        def update_finished_set(
            req_ids: set[str] | None,
            remaining_count_dict: dict[str, int],
            finished_set: set[str],
        ) -> None:
            for req_id in req_ids or ():
                remaining_count = remaining_count_dict.get(
                    req_id, self._expected_finished_count
                )
                remaining_count_dict[req_id] = remaining_count - 1
                if remaining_count_dict[req_id] == 0:
                    finished_set.add(req_id)
                    del remaining_count_dict[req_id]
                    
        finished_sending = set[str]()
        finished_recving = set[str]()

        for model_runner_output in outputs:
            assert model_runner_output is not None
            kv_output = model_runner_output.kv_connector_output
            if not kv_output:
                continue
            # Allow the worker to dynamically update the expected number of
            # finished sending/recving for new requests.
            if (
                kv_output.expected_finished_count > 0
                and kv_output.expected_finished_count != self._expected_finished_count
            ):
              
                self._expected_finished_count = kv_output.expected_finished_count

            update_finished_set(
                kv_output.finished_sending, self._send_remaining_count, finished_sending
            )
            update_finished_set(
                kv_output.finished_recving, self._recv_remaining_count, finished_recving
            )
            # Aggregate kv_connector_stats from all workers.
            if aggregated_kv_connector_stats is None:
                # Use the first worker's kv_connector_stats as accumulator.
                aggregated_kv_connector_stats = kv_output.kv_connector_stats
            elif kv_connector_stats := kv_output.kv_connector_stats:
                if aggregated_kv_connector_stats is None:
                    aggregated_kv_connector_stats = kv_connector_stats
                else:
                    assert isinstance(
                        aggregated_kv_connector_stats, type(kv_connector_stats)
                    )
                    aggregated_kv_connector_stats = (
                        aggregated_kv_connector_stats.aggregate(kv_connector_stats)
                    )
                    
            invalid_block_ids |= kv_output.invalid_block_ids
        # select output of the worker specified by output_rank
        output = outputs[output_rank]
        assert output is not None
        # output.kv_connector_output = KVConnectorOutput(
        #     finished_sending=finished_sending or None,
        #     finished_recving=finished_recving or None,
        #     kv_connector_stats=aggregated_kv_connector_stats or None,
        #     kv_cache_events=combined_kv_cache_events or None,
        #     invalid_block_ids=invalid_block_ids,
        #     expected_finished_count=self._expected_finished_count,
        # )
        return finished_sending,finished_recving