import sqlite3
import re
import pandas as pd
import spacy


class DbManager:
    def __init__(self, database_name: str):
        self.nlp = spacy.load("ru_core_news_sm")
        self.db = sqlite3.connect(database_name)

        self.cur = self.db.cursor()
        self.cur.execute('''SELECT * FROM token
            JOIN texts ON token.text_id = texts.id
            JOIN sentences ON token.sentence_id = sentences.id''')
        tokens = self.cur.fetchall() #3 - токен, 4 - лемма, 5 - часть речи, 7 - текст, 8 - тип текста, 9 - источник, 12 - предложение

    def check_query(self, query): #функция для проверки типа запроса
        if not query.startswith(('"', 'A', 'C', 'N', 'D', 'P', 'S', 'V', 'I')) and not '+' in query:
            doc = self.nlp(query)
            lemma = doc[0].lemma_
            return lemma
        else:
            return query

    def search_each(self, query): #функция поиска в базе данных
        checked = self.check_query(query)
        res = ""
        if '+' in checked:
            query_wordwithpos = '''
            SELECT * FROM token
            JOIN texts ON token.text_id = texts.id
            JOIN sentences ON token.sentence_id = sentences.id
            WHERE token = ? and pos = ?
            '''  #запрос для случаев знать+NOUN, первая часть -- конкретная словоформа
            checked = checked.split('+')
            token = checked[0]
            pos = checked[1]
            self.cur.execute(query_wordwithpos, (token, pos))
            res = self.cur.fetchall()
        elif checked.startswith('"'):
            checked = checked.strip('"')
            query_exactform = '''
            SELECT * FROM token
            JOIN texts ON token.text_id = texts.id
            JOIN sentences ON token.sentence_id = sentences.id
        WHERE token = ?
        ''' #запрос для поиска по конкретной словоформе
            self.cur.execute(query_exactform, (checked,)) 
            res = self.cur.fetchall()
        elif checked.startswith(('A', 'C', 'N', 'D', 'P', 'S', 'V', 'I')):
            query_pos = '''
            SELECT * FROM token
            JOIN texts ON token.text_id = texts.id
            JOIN sentences ON token.sentence_id = sentences.id
        WHERE pos = ?
        ''' #запрос для поиска по части речи
            self.cur.execute(query_pos, (checked,))
            res = self.cur.fetchall()
        else:
            query_lemma = '''
            SELECT * FROM token
            JOIN texts ON token.text_id = texts.id
            JOIN sentences ON token.sentence_id = sentences.id
            WHERE lem = ?
            ''' #запрос для поиска по лемме
            self.cur.execute(query_lemma, (checked,))
            res = self.cur.fetchall()
        return res

    def search(self, queries):
        queries = queries.split()
        result = []
        if len(queries) == 1:
            tokens = self.search_each(queries[0])
            for token in tokens:
                result.append(
                    {
                        'result': token[3],
                        'sentence': token[12],
                        'theme': token[8],
                        'source': token[9]
                    }
                )
        elif len(queries) == 2:
            first = self.search_each(queries[0])
            second = self.search_each(queries[1])
            for i in first: #перебираем результаты для двух запросов
                for j in second:
                    if j[0]-i[0] == 1: #смотрим расстояние между токенами, если 1 -- выдаем результат
                        res = i[3] + ' ' + j[3]
                        result.append(
                            {
                                'result': res,
                                'sentence': i[12],
                                'theme': i[8],
                                'source': i[9]
                            }
                        )
        else:
            first = self.search_each(queries[0])
            second = self.search_each(queries[1])
            third = self.search_each(queries[2])
            for i in first:
                for j in second:
                    if j[0]-i[0] == 1: #прежде чем перебирать результаты третьего запроса, отсекаем случаи, когда между первыми двумя токенами расстояние не 1
                        for z in third:
                            if z[0]-j[0] == 1: #если расстояние между токенами 2 и 3 равно 1, выдаем результат
                                res = i[3] + ' ' + j[3] + ' ' + z[3]
                                result.append(
                                    {
                                        'result': res,
                                        'sentence': i[12],
                                        'theme': i[8],
                                        'source': i[9]
                                    }
                                )
        return result

    def close_connection(self):
        self.db.close()
