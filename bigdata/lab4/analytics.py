import json
import networkx as nx
from collections import defaultdict
from storage import DataStorage

class MapReducePageRank:
    def __init__(self, storage: DataStorage, dump_file="mr_graph.jsonl"):
        self.storage = storage
        self.dump_file = dump_file
        self.damping = 0.85

    def export_graph(self):
        """Выгрузка графа из БД в файл для эмуляции MapReduce"""
        conn = self.storage.get_conn()
        docs = conn.execute("SELECT id, url FROM documents WHERE content IS NOT NULL").fetchall()
        
        with open(self.dump_file, "w", encoding="utf-8") as f:
            for doc_id, url in docs:
                targets = conn.execute("SELECT to_id FROM links WHERE from_id=?", (doc_id,)).fetchall()
                out_links = [t[0] for t in targets]
                node = {"rank": 1.0, "out": out_links, "url": url}
                f.write(f"{doc_id}\t{json.dumps(node)}\n")
        conn.close()

    def run(self, iterations=10):
        print(f"Running MapReduce PageRank for {iterations} iterations...")
        nodes = self._load_nodes()
        
        for i in range(iterations):
            # MAP PHASE
            emitted = defaultdict(list)
            for doc_id, data in nodes.items():
                emitted[doc_id].append(("NODE", data))
                rank = data["rank"]
                outs = data["out"]
                if outs:
                    share = rank / len(outs)
                    for target in outs:
                        emitted[str(target)].append(("RANK", share))
            
            # REDUCE PHASE
            new_nodes = {}
            for doc_id, messages in emitted.items():
                node_struct = None
                rank_sum = 0.0
                for msg_type, payload in messages:
                    if msg_type == "NODE":
                        node_struct = payload
                    elif msg_type == "RANK":
                        rank_sum += payload
                
                if node_struct:
                    new_rank = (1 - self.damping) + self.damping * rank_sum
                    node_struct["rank"] = new_rank
                    new_nodes[doc_id] = node_struct
            
            nodes = new_nodes

        print("MapReduce PageRank finished.")
        return nodes

    def _load_nodes(self):
        nodes = {}
        with open(self.dump_file, "r") as f:
            for line in f:
                did, payload = line.strip().split("\t", 1)
                nodes[did] = json.loads(payload)
        return nodes


class PregelPageRank:
    def __init__(self, storage: DataStorage):
        self.storage = storage
        self.damping = 0.85

    def run(self, iterations=20):
        print(f"Running Pregel (Custom Vertex-Centric) PageRank for {iterations} iterations...")
        G = nx.DiGraph()
        conn = self.storage.get_conn()
        
        # 1. Загрузка графа в память
        docs = conn.execute("SELECT id, url FROM documents WHERE content IS NOT NULL").fetchall()
        for did, url in docs:
            G.add_node(did, url=url)
        
        links = conn.execute("SELECT from_id, to_id FROM links").fetchall()
        for src, dst in links:
            if G.has_node(src) and G.has_node(dst):
                G.add_edge(src, dst)
        conn.close()

        num_nodes = G.number_of_nodes()
        if num_nodes == 0:
            return {}

        # 2. Инициализация рангов (Superstep 0)
        # Каждый узел начинает с весом 1/N
        ranks = {node: 1.0 / num_nodes for node in G.nodes()}

        for i in range(iterations):
            # Сообщения, отправленные соседям (инициализируем нулями)
            incoming_messages = {node: 0.0 for node in G.nodes()}
            
            # Сумма рангов "тупиковых" узлов (dangling nodes), у которых нет исходящих ссылок.
            # В PageRank их вес распределяется равномерно между всеми узлами.
            dangling_sum = 0.0

            # Фаза "Compute" для каждого узла (эмуляция параллелизма)
            for node in G.nodes():
                current_rank = ranks[node]
                out_edges = list(G.successors(node))
                out_degree = len(out_edges)

                if out_degree == 0:
                    dangling_sum += current_rank
                else:
                    share = current_rank / out_degree
                    for neighbor in out_edges:
                        incoming_messages[neighbor] += share

            # Обновление состояния (State Update)
            # PR(A) = (1-d)/N + d * (сумма входящих + распределенный dangling)
            teleport_prob = (1 - self.damping) / num_nodes
            dangling_share = (self.damping * dangling_sum) / num_nodes

            for node in G.nodes():
                ranks[node] = teleport_prob + (self.damping * incoming_messages[node]) + dangling_share

        print("Pregel PageRank finished.")
        return ranks