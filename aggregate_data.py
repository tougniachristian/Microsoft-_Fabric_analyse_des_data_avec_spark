from pyspark.sql.functions import *

# Quantité par produit
productSales = df.select("Item", "Quantity").groupBy("Item").sum()
display(productSales)

# Commandes par année
yearlySales = df.select(year(col("OrderDate")).alias("Year")).groupBy("Year").count().orderBy("Year")
display(yearlySales)