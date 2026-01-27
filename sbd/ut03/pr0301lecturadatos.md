```python
import pandas as pd
import json
```


```python
# 1
df_norte = pd.read_csv('data/ventas_norte.csv', sep=';')
df_norte
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ID_Transaccion</th>
      <th>Fecha_Venta</th>
      <th>Nom_Producto</th>
      <th>Cantidad_Vendida</th>
      <th>Precio_Unit</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1000</td>
      <td>2023-02-21</td>
      <td>Laptop</td>
      <td>4</td>
      <td>423</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1001</td>
      <td>2023-01-15</td>
      <td>Laptop</td>
      <td>2</td>
      <td>171</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1002</td>
      <td>2023-03-13</td>
      <td>Laptop</td>
      <td>3</td>
      <td>73</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1003</td>
      <td>2023-03-02</td>
      <td>Teclado</td>
      <td>1</td>
      <td>139</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1004</td>
      <td>2023-01-21</td>
      <td>Monitor</td>
      <td>4</td>
      <td>692</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>95</th>
      <td>1095</td>
      <td>2023-02-10</td>
      <td>Laptop</td>
      <td>3</td>
      <td>516</td>
    </tr>
    <tr>
      <th>96</th>
      <td>1096</td>
      <td>2023-01-29</td>
      <td>Monitor</td>
      <td>3</td>
      <td>321</td>
    </tr>
    <tr>
      <th>97</th>
      <td>1097</td>
      <td>2023-01-15</td>
      <td>Laptop</td>
      <td>4</td>
      <td>200</td>
    </tr>
    <tr>
      <th>98</th>
      <td>1098</td>
      <td>2023-02-14</td>
      <td>Mouse</td>
      <td>4</td>
      <td>626</td>
    </tr>
    <tr>
      <th>99</th>
      <td>1099</td>
      <td>2023-03-06</td>
      <td>Mouse</td>
      <td>3</td>
      <td>118</td>
    </tr>
  </tbody>
</table>
<p>100 rows × 5 columns</p>
</div>




```python
# 2
hojas_dict = pd.read_excel('data/ventas_sur.xlsx', sheet_name=None)
lista_dataframes = list(hojas_dict.values())
df_sur = pd.concat(lista_dataframes, ignore_index=True)
df_sur
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>SalesID</th>
      <th>Date</th>
      <th>Item</th>
      <th>Qty</th>
      <th>UnitPrice</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2000</td>
      <td>2023-03-01</td>
      <td>Monitor</td>
      <td>6</td>
      <td>624</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2001</td>
      <td>2023-03-04</td>
      <td>Laptop</td>
      <td>7</td>
      <td>941</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2002</td>
      <td>2023-03-26</td>
      <td>Mouse</td>
      <td>3</td>
      <td>989</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2003</td>
      <td>2023-02-01</td>
      <td>Webcam</td>
      <td>3</td>
      <td>621</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2004</td>
      <td>2023-03-28</td>
      <td>Mouse</td>
      <td>5</td>
      <td>437</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>145</th>
      <td>2045</td>
      <td>2023-03-25</td>
      <td>Mouse</td>
      <td>5</td>
      <td>192</td>
    </tr>
    <tr>
      <th>146</th>
      <td>2046</td>
      <td>2023-03-29</td>
      <td>Teclado</td>
      <td>9</td>
      <td>319</td>
    </tr>
    <tr>
      <th>147</th>
      <td>2047</td>
      <td>2023-03-10</td>
      <td>Laptop</td>
      <td>1</td>
      <td>664</td>
    </tr>
    <tr>
      <th>148</th>
      <td>2048</td>
      <td>2023-02-03</td>
      <td>Laptop</td>
      <td>5</td>
      <td>345</td>
    </tr>
    <tr>
      <th>149</th>
      <td>2049</td>
      <td>2023-01-06</td>
      <td>Monitor</td>
      <td>6</td>
      <td>429</td>
    </tr>
  </tbody>
</table>
<p>150 rows × 5 columns</p>
</div>




```python
# 3
with open("data/ventas_este.json") as f:
    data = json.load(f)

df_este = pd.json_normalize(data)
df_este
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id_orden</th>
      <th>timestamp</th>
      <th>detalles_producto.nombre</th>
      <th>detalles_producto.categoria</th>
      <th>detalles_producto.specs.cantidad</th>
      <th>detalles_producto.specs.precio</th>
      <th>cliente.nombre</th>
      <th>cliente.email</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ORD-3000</td>
      <td>2023-03-09 00:00:00</td>
      <td>Monitor</td>
      <td>Electrónica</td>
      <td>2</td>
      <td>244</td>
      <td>Cliente_0</td>
      <td>cliente0@mail.com</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ORD-3001</td>
      <td>2023-01-20 00:00:00</td>
      <td>Laptop</td>
      <td>Electrónica</td>
      <td>2</td>
      <td>578</td>
      <td>Cliente_1</td>
      <td>cliente1@mail.com</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ORD-3002</td>
      <td>2023-01-01 00:00:00</td>
      <td>Mouse</td>
      <td>Electrónica</td>
      <td>2</td>
      <td>339</td>
      <td>Cliente_2</td>
      <td>cliente2@mail.com</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ORD-3003</td>
      <td>2023-02-07 00:00:00</td>
      <td>Webcam</td>
      <td>Electrónica</td>
      <td>2</td>
      <td>158</td>
      <td>Cliente_3</td>
      <td>cliente3@mail.com</td>
    </tr>
    <tr>
      <th>4</th>
      <td>ORD-3004</td>
      <td>2023-03-18 00:00:00</td>
      <td>Monitor</td>
      <td>Electrónica</td>
      <td>1</td>
      <td>692</td>
      <td>Cliente_4</td>
      <td>cliente4@mail.com</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>95</th>
      <td>ORD-3095</td>
      <td>2023-03-28 00:00:00</td>
      <td>Webcam</td>
      <td>Electrónica</td>
      <td>1</td>
      <td>857</td>
      <td>Cliente_95</td>
      <td>cliente95@mail.com</td>
    </tr>
    <tr>
      <th>96</th>
      <td>ORD-3096</td>
      <td>2023-01-18 00:00:00</td>
      <td>Webcam</td>
      <td>Electrónica</td>
      <td>2</td>
      <td>375</td>
      <td>Cliente_96</td>
      <td>cliente96@mail.com</td>
    </tr>
    <tr>
      <th>97</th>
      <td>ORD-3097</td>
      <td>2023-02-10 00:00:00</td>
      <td>Mouse</td>
      <td>Electrónica</td>
      <td>1</td>
      <td>696</td>
      <td>Cliente_97</td>
      <td>cliente97@mail.com</td>
    </tr>
    <tr>
      <th>98</th>
      <td>ORD-3098</td>
      <td>2023-01-25 00:00:00</td>
      <td>Mouse</td>
      <td>Electrónica</td>
      <td>2</td>
      <td>618</td>
      <td>Cliente_98</td>
      <td>cliente98@mail.com</td>
    </tr>
    <tr>
      <th>99</th>
      <td>ORD-3099</td>
      <td>2023-03-27 00:00:00</td>
      <td>Webcam</td>
      <td>Electrónica</td>
      <td>1</td>
      <td>844</td>
      <td>Cliente_99</td>
      <td>cliente99@mail.com</td>
    </tr>
  </tbody>
</table>
<p>100 rows × 8 columns</p>
</div>




```python
# 4
df_norte.rename(
    columns={"ID_Transaccion":"ID", "Fecha_Venta":"fecha", "Nom_Producto":"producto", "Cantidad_Vendida":"cantidad", "Precio_Unit":"precio_unitario"}, 
    inplace=True
)
df_sur.rename(
    columns={"SalesID":"ID", "Date":"fecha", "Item":"producto", "Qty":"cantidad", "UnitPrice":"precio_unitario"}, 
    inplace=True
)
df_este.rename(
    columns={"id_orden":"ID", "timestamp":"fecha", "detalles_producto.nombre":"producto", "detalles_producto.specs.cantidad":"cantidad", "detalles_producto.specs.precio":"precio_unitario"}, 
    inplace=True
)

df_este.drop(
    columns=["detalles_producto.categoria", "cliente.nombre", "cliente.email"],
    inplace=True
)

df_norte["region"] = "Norte"
df_sur["region"] = "Sur"
df_este["region"] = "Este"
```


```python
# 5
df_total = pd.concat([df_norte, df_sur, df_este])
df_total
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ID</th>
      <th>fecha</th>
      <th>producto</th>
      <th>cantidad</th>
      <th>precio_unitario</th>
      <th>region</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1000</td>
      <td>2023-02-21</td>
      <td>Laptop</td>
      <td>4</td>
      <td>423</td>
      <td>Norte</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1001</td>
      <td>2023-01-15</td>
      <td>Laptop</td>
      <td>2</td>
      <td>171</td>
      <td>Norte</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1002</td>
      <td>2023-03-13</td>
      <td>Laptop</td>
      <td>3</td>
      <td>73</td>
      <td>Norte</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1003</td>
      <td>2023-03-02</td>
      <td>Teclado</td>
      <td>1</td>
      <td>139</td>
      <td>Norte</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1004</td>
      <td>2023-01-21</td>
      <td>Monitor</td>
      <td>4</td>
      <td>692</td>
      <td>Norte</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>95</th>
      <td>ORD-3095</td>
      <td>2023-03-28 00:00:00</td>
      <td>Webcam</td>
      <td>1</td>
      <td>857</td>
      <td>Este</td>
    </tr>
    <tr>
      <th>96</th>
      <td>ORD-3096</td>
      <td>2023-01-18 00:00:00</td>
      <td>Webcam</td>
      <td>2</td>
      <td>375</td>
      <td>Este</td>
    </tr>
    <tr>
      <th>97</th>
      <td>ORD-3097</td>
      <td>2023-02-10 00:00:00</td>
      <td>Mouse</td>
      <td>1</td>
      <td>696</td>
      <td>Este</td>
    </tr>
    <tr>
      <th>98</th>
      <td>ORD-3098</td>
      <td>2023-01-25 00:00:00</td>
      <td>Mouse</td>
      <td>2</td>
      <td>618</td>
      <td>Este</td>
    </tr>
    <tr>
      <th>99</th>
      <td>ORD-3099</td>
      <td>2023-03-27 00:00:00</td>
      <td>Webcam</td>
      <td>1</td>
      <td>844</td>
      <td>Este</td>
    </tr>
  </tbody>
</table>
<p>350 rows × 6 columns</p>
</div>




```python
# 6
df_total.to_csv(
    "ventas_consolidadas.csv",
    sep=",",
    encoding="utf-8",
    index=False
)
```

___

[**ventas_consolidadas.csv**](./ventas_consolidadas.csv)
