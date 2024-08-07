import pandas as pd
import os
import zipfile
import requests
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from urllib.parse import urljoin, urlparse
import mimetypes
import warnings
import re
from unidecode import unidecode

# Suppress specific BeautifulSoup warning
warnings.filterwarnings('ignore', category=MarkupResemblesLocatorWarning)

# Load the CSV file
csv_path = 'articles_to_migrate.csv'  # Update this path as needed
df = pd.read_csv(csv_path)

# Create the base directory for knowledge articles
base_dir = 'knowledge_articles'
os.makedirs(base_dir, exist_ok=True)

def sanitize_string(value):
    """Sanitize a string to be used as a valid filename or URL name."""
    if not isinstance(value, str):
        value = str(value)  # Convert to string
        print(f"Converted to stiring this title: {value}")

    value = unidecode(value)  # Convert to closest ASCII representation
    value = re.sub(r'[^\w\s-]', '', value).strip().lower()
    value = re.sub(r'[-\s]+', '_', value)
    return value

def download_asset(url, download_dir):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Check if the request was successful
    except (requests.exceptions.MissingSchema, requests.exceptions.InvalidURL) as e:
        print(f"Skipping asset download due to invalid URL: {url} - {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Skipping asset download due to request error: {url} - {e}")
        return None

    content_type = response.headers.get('Content-Type')
    if content_type:
        ext = mimetypes.guess_extension(content_type.split(';')[0])
    else:
        ext = ''
    
    filename = os.path.basename(urlparse(url).path)
    if not filename:
        filename = 'asset'
    
    if not os.path.splitext(filename)[1]:
        filename += ext

    file_path = os.path.join(download_dir, filename)
    
    with open(file_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=128):
            f.write(chunk)
    
    return os.path.relpath(file_path, start=download_dir)

# Sanitize the UrlName column
#df['UrlName'] = df['UrlName'].apply(sanitize_string)

# Iterate over the DataFrame and create HTML files and asset subdirectories
for index, row in df.iterrows():
    article_id = row['ExternalId__c']
    article_lang = row['Language']
    article_title = sanitize_string(row['Title'])
    
    # Create subdirectory for each article
    article_dir = os.path.join(base_dir, f"{article_lang}_{article_id}")
    os.makedirs(article_dir, exist_ok=True)
    
    # Create subdirectory for assets within the article directory
    article_assets_dir = os.path.join(article_dir, 'assets')
    os.makedirs(article_assets_dir, exist_ok=True)
    
    html_filename = f"{article_lang}_{article_id}.html"
    html_filepath = os.path.join(article_dir, html_filename)
    
    # Parse the HTML content and download assets
    try:
        soup = BeautifulSoup(row['Answer__c'], 'html.parser')
    except Exception as e:
        print(f"Skipping article {article_id} due to parsing error: {e}")
        continue
    
    # Download and replace images
    for img in soup.find_all('img'):
        img_url = img['src']
        local_img_path = download_asset(img_url, article_assets_dir)
        if local_img_path:
            img['src'] = os.path.join('assets', os.path.basename(local_img_path))
    
    # Replace specific <div class="ssep-video"> tags with <iframe>
    for div in soup.find_all('div', class_='ssep-video'):
        script_tag = div.find('script', src=True)
        if script_tag:
            video_url = script_tag['src']
            video_id = video_url.split('/embed/medias/')[1].split('.jsonp')[0]
            iframe_tag = soup.new_tag('p')
            iframe_tag.append(soup.new_tag('iframe', width='915', height='515', frameborder='0', scrolling='auto', 
                                           src=f'https://fast.wistia.net/embed/iframe/{video_id}?seo=true&amp;videoFoam=true', 
                                           title=article_title, 
                                           allowfullscreen='allowfullscreen'))
            div.replace_with(iframe_tag)

    # Replace specific <div class="wistia_responsive_padding"> tags with <iframe>
    for div in soup.find_all('div', class_='wistia_responsive_padding'):
        iframe_tag = div.find('iframe', src=True)
        if iframe_tag:
            video_url = iframe_tag['src']
            iframe_new = soup.new_tag('iframe', width='915', height='515', frameborder='0', scrolling='auto',
                                      src=f'{video_url}',
                                      title=article_title,
                                      allowfullscreen='allowfullscreen')
            p_tag = soup.new_tag('p')
            p_tag.append(iframe_new)
            div.replace_with(p_tag)

    # Write the updated HTML content to a file
    with open(html_filepath, 'w', encoding='utf-8') as f:
        f.write(str(soup))
    
    # Update the DataFrame with the relative path to the HTML file
    #df.at[index, 'Answer__c'] = os.path.relpath(html_filepath, start=base_dir)
    df.at[index, 'Answer__c'] = html_filepath

# Save the updated CSV with relative paths and sanitized UrlName
updated_csv_path = os.path.join(base_dir, 'articles_to_migrate.csv')
df.to_csv(updated_csv_path, index=False)

# Zip the articles directory
zip_filename = 'knowledge_articles.zip'
with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            zipf.write(os.path.join(root, file),
                       os.path.relpath(os.path.join(root, file),
                                       os.path.join(base_dir, '..')))

print(f"Knowledge articles have been zipped into {zip_filename}")
