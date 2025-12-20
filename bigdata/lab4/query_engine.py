import math
from storage import DataStorage

class SearchEngine:
    def __init__(self, storage: DataStorage):
        self.storage = storage

    def _get_idf_params(self, term):
        conn = self.storage.get_conn()
        cur = conn.cursor()
        
        N = cur.execute("SELECT COUNT(*) FROM documents WHERE content IS NOT NULL").fetchone()[0]
        if N == 0: N = 1
        
        term_row = cur.execute("SELECT id FROM terms WHERE term=?", (term,)).fetchone()
        if not term_row:
            conn.close()
            return 0, None
        
        term_id = term_row[0]
        df = cur.execute("SELECT COUNT(DISTINCT doc_id) FROM postings WHERE term_id=?", (term_id,)).fetchone()[0]
        conn.close()
        
        # TF-IDF формула
        idf = math.log(1 + (N / (df + 1))) 
        return idf, term_id

    def search_taat(self, query):
        """
        TAAT (Term-at-a-Time). 
        Логика: TF-IDF скоринг. 
        Результат: Дробное число (релевантность).
        """
        print(f"\n[TAAT] Searching for: '{query}' (Method: TF-IDF)")
        terms = query.lower().split()
        scores = {}
        
        conn = self.storage.get_conn()
        
        for term in terms:
            idf, term_id = self._get_idf_params(term)
            if not term_id: continue
            
            rows = conn.execute("SELECT doc_id, freq FROM postings WHERE term_id=?", (term_id,)).fetchall()
            for doc_id, freq in rows:
                # TF = 1 + log(freq)
                tf = 1 + math.log(freq) if freq > 0 else 0
                
                if doc_id not in scores: scores[doc_id] = 0.0
                scores[doc_id] += tf * idf
        
        conn.close()
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)

    def search_daat(self, query):
        """
        DAAT (Document-at-a-Time).
        Логика: Пересечение списков (AND).
        Результат: Сумма частот (Simple Frequency Sum).
        """
        print(f"\n[DAAT] Searching for: '{query}' (Method: Simple Freq Sum)")
        terms = query.lower().split()
        
        term_data = []
        conn = self.storage.get_conn()
        
        # 1. Сбор данных
        for term in terms:
            # Нам нужен term_id, но IDF здесь не важен
            term_row = conn.execute("SELECT id FROM terms WHERE term=?", (term,)).fetchone()
            if not term_row:
                conn.close()
                return []
            
            term_id = term_row[0]
            rows = conn.execute("SELECT doc_id, freq FROM postings WHERE term_id=? ORDER BY doc_id", (term_id,)).fetchall()
            term_data.append({
                "postings": rows,
                "cursor": 0,
                "len": len(rows)
            })
        
        results = []
        if not term_data:
            conn.close()
            return []

        while True:
            if any(t["cursor"] >= t["len"] for t in term_data):
                break

            current_doc_ids = [t["postings"][t["cursor"]][0] for t in term_data]
            
            if all(d == current_doc_ids[0] for d in current_doc_ids):
                match_doc_id = current_doc_ids[0]
                freq_sum = 0
                
                # Просто суммируем частоты (насколько часто слова встречаются)
                for t in term_data:
                    freq = t["postings"][t["cursor"]][1]
                    freq_sum += freq
                    t["cursor"] += 1
                
                results.append((match_doc_id, freq_sum))
            else:
                # Двигаем курсор отстающего
                min_doc_id = min(current_doc_ids)
                for t in term_data:
                    if t["postings"][t["cursor"]][0] == min_doc_id:
                        t["cursor"] += 1

        conn.close()
        return sorted(results, key=lambda x: x[1], reverse=True)
    
    def print_results(self, results, top_k=5):
        conn = self.storage.get_conn()
        if not results:
            print("  No results found.")
            conn.close()
            return

        for doc_id, score in results[:top_k]:
            url_row = conn.execute("SELECT url FROM documents WHERE id=?", (doc_id,)).fetchone()
            url = url_row[0] if url_row else "Unknown"
            
            score_str = f"{int(score)}" if isinstance(score, int) else f"{score:.4f}"
            print(f"  Score: {score_str} | DocID: {doc_id} | {url}")
        conn.close()