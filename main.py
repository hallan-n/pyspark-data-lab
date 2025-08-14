from bronze.bronze import Bronze
from silver.silver import Silver
from gold.gold import Gold

bronze = Bronze()
silver = Silver()
gold = Gold()

if __name__ == "__main__":
    bronze.run()
    bronze.show()

    silver.run()
    silver.show()

    gold.run()
    gold.show()