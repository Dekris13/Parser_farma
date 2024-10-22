from bs4 import BeautifulSoup
import datetime

class Parser_Farma_Zdorovie():
    
    def __init__(self, response, category, sourse) -> None:

        self.response = response
        self.category = category
        self.sourse = sourse

    def ClearingPrices(self, prices):

        numbers = ['0','1','2','3','4','5','6','7','8','9']
        
        clearPrices = []

        for item in prices:

            finalPrice = str()
            price = item.text

            for i in price:
                if i in numbers:
                    finalPrice = finalPrice+i
            finalPrice = int(finalPrice)        
            clearPrices.append(finalPrice)
        return clearPrices

    def ClearingNames(self, names):

        clearNames = []

        for item in names:

            name = str(item.text)
            name = name.replace("\t", "").replace("\n", "")
            clearNames.append(name)
        return clearNames
    
    def StartParsing(self):

        # Парсим HTML при помощи Beautiful Soup
        soup = BeautifulSoup(self.response.text, 'html.parser')

        #Вычитываем имена
        names = soup.select("div.entry__name > a")

        #Вычитываем цену, если товар есть в наличии  сделать
        prices = soup.select("span.product-item-price-current")

        #Вычитываем отсутсвующие товары, по ним ставим цену 0
        out_of_stock = soup.select("span.no-item")

        #Отчищаем имена от табуляции и перносов
        clearNames = self.ClearingNames(names)
                            
        #Отчищаем цены до int
        clearPrices = self.ClearingPrices(prices)

        #Объединяем список цени и списк отсутствующих товаров, по которым цена 0
        for i in out_of_stock:
            clearPrices.append(0)

        #Подготавливаем данные для загрузки в БД
        ind = 0
        for i in clearNames:
            _data = [clearNames[ind], clearPrices[ind], self.category, self.sourse, datetime.date.today()]
            ind+=1

        return _data
                






