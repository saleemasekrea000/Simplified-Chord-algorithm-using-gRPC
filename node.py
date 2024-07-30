import grpc
import sys
import zlib
from concurrent import futures
import chord_pb2_grpc as pb2_grpc
import chord_pb2 as pb2
import time

CHORD = [2, 16, 24, 25, 26, 31]
CHANNELS = [
    "127.0.0.1:5000",
    "127.0.0.1:5001",
    "127.0.0.1:5002",
    "127.0.0.1:5003",
    "127.0.0.1:5004",
    "127.0.0.1:5005",
]
node_id = sys.argv[1]
data = {}
finger_table = []

M = 5
id = CHORD[int(node_id)]
succ = CHORD[(int(node_id) + 1) % (M + 1)]
pred = CHORD[(int(node_id) - 1) % (M + 1)]


def populate_finger_table(id):

    def find_successor(target):
        for node in CHORD:
            if node >= target:
                return node
        return CHORD[0]

    def find_predecessor(target):
        pred = None
        for node in CHORD:
            if node < target:
                pred = node
            else:
                break
        if pred is None:
            pred = CHORD[-1]
        return pred

    for i in range(M):
        finger_id = (id + 2**i) % (2**M)
        finger_table.append(find_successor(finger_id))
    return finger_table


def get_stub(channel):
    channel = grpc.insecure_channel(channel)
    return pb2_grpc.ChordStub(channel)


def get_target_id(key):
    hash_value = zlib.adler32(key.encode())
    return hash_value % (2**M)


def save(key, text):
    target_id = get_target_id(key)
    try:
        if pred < target_id <= id:
            data[key] = text
            print(f"Node {id} says: Saved {key}")
            return pb2.SaveDataResponse(node_id=id, status=True)

        elif id < target_id <= succ:
            stub = get_stub(CHANNELS[CHORD.index(succ)])
            print(f"Node {id} says: Save from {id} to {succ}")
            return stub.SaveData(pb2.SaveDataMessage(key=key, text=text))

        else:
            node_MAX = get_node_in_finger_table(target_id)
            stub = get_stub(CHANNELS[CHORD.index(node_MAX)])
            print(f"Node {id} says: Save from {id} to {node_MAX}")
            return stub.SaveData(pb2.SaveDataMessage(key=key, text=text))
    except grpc.RpcError:
        return pb2.SaveDataResponse(node_id=id, status=False)


def remove(key):
    target_id = get_target_id(key)
    try:
        if pred < target_id <= id:
            if key in data:
                del data[key]
                print(f"Node {id} says: Removed {key}")
                return pb2.RemoveDataResponse(node_id=id, status=True)
            else:
                # print("Failure, data was not found")
                return pb2.RemoveDataResponse(node_id=id, status=False)

        elif id < target_id <= succ:
            stub = get_stub(CHANNELS[CHORD.index(succ)])
            print(f"Node {id} says: Remove from {id} to {succ}")
            response = stub.RemoveData(pb2.RemoveDataMessage(key=key))
            return response

        else:
            node_MAX = get_node_in_finger_table(target_id)
            stub = get_stub(CHANNELS[CHORD.index(node_MAX)])
            print(f"Node {id} says: Remove from {id} to {node_MAX}")
            response = stub.RemoveData(pb2.RemoveDataMessage(key=key))
            return response
    except grpc.RpcError:
        return pb2.SaveDataResponse(node_id=id, status=False)


def find(key):
    target_id = get_target_id(key)
    try:
        if pred < target_id <= id:
            if key in data:
                value = data[key]
                print(f"Node {id} says: Found {key}")
                return pb2.FindDataResponse(node_id=id, data=value)
            else:
                # print("Error: Key not found")
                return pb2.FindDataResponse(node_id=id, data=None)

        elif id < target_id <= succ:
            stub = get_stub(CHANNELS[CHORD.index(succ)])
            print(f"Node {id} says: Find from {id} to {succ}")
            response = stub.FindData(pb2.FindDataMessage(key=key))
            return response

        else:
            node_MAX = get_node_in_finger_table(target_id)
            stub = get_stub(CHANNELS[CHORD.index(node_MAX)])
            print(f"Node {id} says: Find from {id} to {node_MAX}")
            response = stub.FindData(pb2.FindDataMessage(key=key))
            return response
    except grpc.RpcError:
        return pb2.FindDataResponse(node_id=id, data=None)


class NodeHandler(pb2_grpc.ChordServicer):
    def SaveData(self, request, context):
        return save(request.key, request.text)

    def RemoveData(self, request, context):
        return remove(request.key)

    def FindData(self, request, context):
        return find(request.key)

    def GetFingerTable(self, request, context):
        response = pb2.GetFingerTableResponse(finger_table=finger_table)
        return response


def get_node_in_finger_table(target_id):
    largest_smaller_node = -1
    for node_id in finger_table:
        if node_id < target_id:
            if largest_smaller_node is None or node_id > largest_smaller_node:
                largest_smaller_node = node_id
    return largest_smaller_node


if __name__ == "__main__":

    node_port = str(5000 + int(node_id))
    node = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ChordServicer_to_server(NodeHandler(), node)
    node.add_insecure_port("127.0.0.1:" + node_port)
    node.start()
    print(f"Node {id} {populate_finger_table(id)}")
    try:
        node.wait_for_termination()
    except KeyboardInterrupt:
        print("Shutting down")
