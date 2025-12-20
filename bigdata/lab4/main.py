from storage import DataStorage
from spider import WikiSpider
from analytics import MapReducePageRank, PregelPageRank
from query_engine import SearchEngine

def main():
    # 1. Инициализация БД
    store = DataStorage()
    
    # Спрашиваем, надо ли заново качать данные
    if input("Clear DB and crawl fresh data? (y/n): ").lower() == 'y':
        store.clear_data()
        spider = WikiSpider(store)
        seeds = [
            "https://en.wikipedia.org/wiki/Big_data",
            "https://en.wikipedia.org/wiki/Data_mining",
            "https://en.wikipedia.org/wiki/Artificial_intelligence"
        ]
        spider.run(seeds, limit=40)

    # 2. PageRank (MapReduce)
    mr_ranker = MapReducePageRank(store)
    mr_ranker.export_graph() # Подготовка данных
    mr_res = mr_ranker.run(iterations=10)
    print("Top 3 MapReduce PR:", sorted(mr_res.items(), key=lambda x: x[1]['rank'], reverse=True)[:3])

    # 3. PageRank (Pregel/Graph)
    pregel_ranker = PregelPageRank(store)
    pg_res = pregel_ranker.run(iterations=100)
    print("Top 3 Pregel PR:", sorted(pg_res.items(), key=lambda x: x[1], reverse=True)[:3])

    # 4. Поиск
    engine = SearchEngine(store)
    
    while True:
        q = input("\nEnter search query (or 'exit'): ")
        if q == 'exit': break
        
        # TAAT
        taat_res = engine.search_taat(q)
        engine.print_results(taat_res)
        
        # DAAT
        daat_res = engine.search_daat(q)
        engine.print_results(daat_res)

if __name__ == "__main__":
    main()