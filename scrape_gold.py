import requests
from bs4 import BeautifulSoup
import pandas as pd

# Website URL
URL = "https://www.goldprice.org/gold-price-history.html"

headers = {
    "User-Agent": "Mozilla/5.0"
}

# Send request
response = requests.get(URL, headers=headers)

if response.status_code == 200:
    print("Website connected successfully ✅")

    soup = BeautifulSoup(response.text, "html.parser")

    # Find the first table (you may need to adjust class name)
    table = soup.find("table")

    rows = table.find_all("tr")

    data = []

    for row in rows[1:]:  # skip header row
        cols = row.find_all("td")

        if len(cols) >= 5:
            date = cols[0].text.strip()
            open_price = cols[1].text.strip()
            high_price = cols[2].text.strip()
            low_price = cols[3].text.strip()
            close_price = cols[4].text.strip()

            data.append([date, open_price, high_price, low_price, close_price])

    # Convert to DataFrame
    df = pd.DataFrame(data, columns=["Date", "Open", "High", "Low", "Close"])

    print(df.head())

    # Save to CSV
    df.to_csv("gold_prices.csv", index=False)
    print("Data saved to gold_prices.csv 🎯")

else:
    print("Failed to connect ❌")
