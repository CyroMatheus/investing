# Web Scraping de Dados de Ações com Python

Este projeto realiza o scraping de informações de ações e índices financeiros a partir do site Investing.com, coletando dados como valores atuais, variações e porcentagens de variação. O script utiliza as bibliotecas `BeautifulSoup`, `lxml`, `aiohttp`, e `asyncio` para acessar e processar as páginas web de forma assíncrona, além de armazenar as informações em arquivos CSV para posterior análise.

## Requisitos

Antes de rodar o script, você precisará ter as seguintes bibliotecas instaladas no seu ambiente:

- `beautifulsoup4`
- `lxml`
- `aiohttp`

Você pode instalar essas bibliotecas usando o `pip`:

```bash
pip install beautifulsoup4 lxml aiohttp
