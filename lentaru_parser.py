import requests
from pprint import pp 
import pandas as pd
import time

bloc_types = {
  'Общество/Россия': 1,
  'Экономика': 4,
  'Силовые структуры': 37,
  'Бывший СССР': 3,
  'Спорт': 8,
  'Забота о себе': 87,
  'Строительство': None,
  'Туризм/Путешествия': 48,
  'Наука и техника': 5,
}

def get_news_batch(params) -> list[dict]:
  url = 'https://lenta.ru/search/v2/process'
  response = requests.get(url, params=params)
  print(response.status_code)
  response = response.json()
  response = response.get('matches', [])
  #news = pd.DataFrame(response.get('matches', []))
  #news.to_csv('test.csv')
  return response

#def save

def scrap_lentaru_by_bloc(offset=0, count=0, batch_size=1_000, bloc=1):
  params = {
    'from': 0, # offset
    'size': 5, # count
    'sort': 2, # sort by date
    'title_only': 0, # search by query substring to False
    'domain': 1, # ?
    'type': 1, # types of articles. 0 - all articles, 1 - only news
    'bloc': bloc, # articles theme, 0 - all themes, 4 - economics
    'query': '', # search query
  }
  batch_size = min(batch_size, count)
  news = None
  for _from in range(offset, offset + count, batch_size):
    try:
      params['from'] = _from
      params['size'] = min(batch_size, count-_from+offset)
      csv_filename = f"data/bloc{bloc}_from{_from}_size{params.get('size')}_offset{offset}_lentaru.csv"
      print('from', params.get('from'), 'size', params.get('size'))
      news = get_news_batch(params)
      news = pd.DataFrame(news)
      print(csv_filename)
      news.to_csv(csv_filename)
      time.sleep(3)
    except Exception as error:
      print(error)
      if news:
        news.to_csv(csv_filename)
      time.sleep(1)

for name, bloc in bloc_types.items():
  if bloc in (1, 4, 37, 3, 8, None): continue
  print(name)
  scrap_lentaru_by_bloc(
    count=10_000,
    batch_size=500,
    bloc=bloc,
  )


