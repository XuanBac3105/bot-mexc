import asyncio
import aiohttp
import re

URL = 'https://www.mexc.co/vi-VN/announcements/17827791531874'

async def main():
    async with aiohttp.ClientSession(headers={'User-Agent':'Mozilla/5.0'}) as session:
        try:
            async with session.get(URL, timeout=15) as r:
                text = await r.text()
        except Exception as e:
            print('Error fetching:', e)
            return

    # print meta og:title
    m = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', text)
    if m:
        print('OG Title:', m.group(1))
    else:
        print('No OG title')

    # Isolate main_text similarly to bot logic
    main_text = text
    m_article = re.search(r"(<article[\s\S]*?</article>)", text, re.IGNORECASE)
    if m_article:
        main_text = m_article.group(1)
    else:
        idx = text.lower().find('relatedarticles')
        if idx != -1:
            main_text = text[:idx]

    # Search for strict pattern inside main_text
    strict_pattern = re.compile(r"Đầu tiên trên thị trường\s*:\s*MEXC niêm yết\s+(.+?)\s*\(([A-Z0-9]{2,10})\)\s*USDT-M Futures\s*vào\s*(\d{1,2}:\d{2}\s*\d{1,2}/\d{1,1}/\d{4})", re.IGNORECASE)
    m2 = strict_pattern.search(main_text)
    if m2:
        print('STRICT MATCH FOUND:')
        print(' name=', m2.group(1))
        print(' ticker=', m2.group(2))
        print(' time=', m2.group(3))
    else:
        print('No strict match in full page')

    # find first 400 chars around occurrences of 'Đầu tiên trên thị trường' or 'MEXC niêm yết'
    keywords = ['Đầu tiên trên thị trường','MEXC niêm yết','USDT-M Futures','Meme+']
    for kw in keywords:
        idx = text.lower().find(kw.lower())
        if idx!=-1:
            start = max(0, idx-200)
            end = min(len(text), idx+200)
            print('\n--- EXCERPT FOR:', kw, '---')
            print(text[start:end])
        else:
            print(f'\nNo occurrence of {kw}')

if __name__=='__main__':
    asyncio.run(main())