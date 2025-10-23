import seaborn as sns
plt.clf()
sns.set_theme(style="whitegrid")
ax = sns.barplot(x="OrderYear", y="GrossRevenue", data=df_sales)
plt.show()
