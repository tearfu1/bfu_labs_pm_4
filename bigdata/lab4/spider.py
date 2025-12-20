import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
import time
import json
from storage import DataStorage

class WikiSpider:
    def __init__(self, storage: DataStorage):
        self.storage = storage
        self.visited = set()
        self.domain = "en.wikipedia.org"
        self.headers = {"User-Agent": "BigDataLabBot/2.0"}

    def normalize(self, text):
        # Оставляем только буквы и цифры, приводим к нижнему регистру
        text = text.lower()
        text = re.sub(r'[^a-z0-9\s]', ' ', text)
        return re.sub(r'\s+', ' ', text).strip()

    def tokenize(self, text):
        return text.split()

    def run(self, seed_urls, limit=30):
        print(f"Starting crawl with limit {limit} pages...")
        queue = list(seed_urls)
        count = 0

        while queue and count < limit:
            url = queue.pop(0)
            
            # Пропускаем, если уже посещали или домен чужой
            if url in self.visited or urlparse(url).netloc != self.domain:
                continue
            
            print(f"Processing [{count+1}/{limit}]: {url}")
            try:
                success = self.process_page(url, queue)
                if success:
                    count += 1
                self.visited.add(url)
                time.sleep(0.5) 
            except Exception as e:
                print(f"Error on {url}: {e}")

        print("Crawling and Indexing finished.")

    def process_page(self, url, queue):
        try:
            resp = requests.get(url, headers=self.headers, timeout=5)
            if resp.status_code != 200:
                return False
        except:
            return False

        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Чистка DOM
        for tag in soup(["script", "style", "table"]):
            tag.decompose()

        title = soup.title.string if soup.title else url
        raw_text = soup.get_text(separator=" ")
        clean_text = self.normalize(raw_text)

        conn = self.storage.get_conn()
        cur = conn.cursor()

        # 1. UPSERT (Update or Insert) документа
        cur.execute("SELECT id FROM documents WHERE url=?", (url,))
        row = cur.fetchone()

        if row:
            doc_id = row[0]
            cur.execute("UPDATE documents SET title=?, content=? WHERE id=?", (title, clean_text, doc_id))
        else:
            cur.execute("INSERT INTO documents(url, title, content) VALUES (?,?,?)", (url, title, clean_text))
            doc_id = cur.lastrowid

        # 2. Индексация (Terms & Postings)
        cur.execute("DELETE FROM postings WHERE doc_id=?", (doc_id,))
        
        tokens = self.tokenize(clean_text)
        term_map = {}
        for idx, token in enumerate(tokens):
            term_map.setdefault(token, []).append(idx)

        for term, positions in term_map.items():
            # Добавляем слово в словарь
            cur.execute("INSERT OR IGNORE INTO terms(term) VALUES (?)", (term,))
            # Получаем его ID
            term_id_row = cur.execute("SELECT id FROM terms WHERE term=?", (term,)).fetchone()
            if term_id_row:
                term_id = term_id_row[0]
                cur.execute("INSERT INTO postings(term_id, doc_id, freq, positions) VALUES (?,?,?,?)",
                            (term_id, doc_id, len(positions), json.dumps(positions)))

        # 3. Ссылки
        out_links = set()
        for a in soup.find_all("a", href=True):
            link = a['href']
            # Фильтруем ссылки только на статьи
            if link.startswith("/wiki/") and ":" not in link:
                full_url = urljoin("https://" + self.domain, link)
                if full_url != url: # Не ссылаться на самого себя
                    out_links.add(full_url)

        for link in out_links:
            # Сначала добавляем заглушку для URL цели, чтобы получить ID
            cur.execute("INSERT OR IGNORE INTO documents(url, title, content) VALUES (?,?,?)", (link, None, None))
            
            target_row = cur.execute("SELECT id FROM documents WHERE url=?", (link,)).fetchone()
            if target_row:
                target_id = target_row[0]
                # Добавляем связь
                cur.execute("INSERT OR IGNORE INTO links(from_id, to_id) VALUES (?,?)", (doc_id, target_id))
            
            if link not in self.visited:
                queue.append(link)

        conn.commit()
        conn.close()
        return True