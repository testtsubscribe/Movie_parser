import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
import time
import os
def get_movie_details(href_link):
    response = requests.get(href_link)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        selected_name = soup.find('meta', attrs={'property': 'twitter:title'}).get('content')
        original_name = soup.find('meta', attrs={'property': 'og:video:tag'}).get('content')
        download_url = soup.find('meta', attrs={'property': 'twitter:image'}).get('content').replace('thumb.jpg', 'video.mp4')
        return selected_name, original_name, download_url
    else:
        return None
def get_movies_list():
    data = []
    language_list = ["az", "ru", "en", "tr"]
    with ThreadPoolExecutor() as executor:
        futures = []
        for language in language_list:
            for page in range(2):
                url = f"https://video.az/movie/lang/{language}?page={page}"
                response = requests.get(url)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    elements = soup.select('.title')
                    for element in elements:
                        href_link = element['href']
                        futures.append(executor.submit(get_movie_details, href_link))
                        data.append({
                            'Dublaj': language,
                            'Href Link': href_link
                        })
                else:
                    # Handle error or log it
                    pass
        for i, future in enumerate(futures):
            result = future.result()
            if result:
                selected_name, original_name, download_url = result
                data[i]['Name'] = selected_name
                data[i]['Original Name'] = original_name
                data[i]['Download URL'] = download_url
    return data
if __name__ == "__main__":
    start_time = time.time()
    result = get_movies_list()
    execution_time = time.time() - start_time
    df_new = pd.DataFrame(result)
    df_new = df_new.rename(columns={'Dublaj': 'Language', 'Name': 'Movie Name', 'Original Name': 'Movie Name in Original', 'Href Link': 'URL', 'Download URL': 'Download URL'})
    df_new = df_new[['Language', 'Movie Name', 'Movie Name in Original', 'URL', 'Download URL']] # Reorder the columns
     # Check if file exists
    if os.path.isfile('movies_data.xlsx'):
        df_old = pd.read_excel('movies_data.xlsx')
        df_final = pd.concat([df_old, df_new]).drop_duplicates().reset_index(drop=True)
    else:
        df_final = df_new
    df_final.to_excel('movies_data.xlsx', index=False)
    print(f"Execution Time: {execution_time} seconds")