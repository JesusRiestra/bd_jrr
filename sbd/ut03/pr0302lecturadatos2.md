```python
import pandas as pd
import json
```


```python
# Norte
df_norte = pd.read_csv(
    'data/ventas_norte_v2.csv', 
    sep=';',
    quotechar='"'
)
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
      <th>ID_Pedido</th>
      <th>Fecha_Transaccion</th>
      <th>Cliente_Nombre</th>
      <th>Direccion_Envio</th>
      <th>Producto</th>
      <th>Unidades</th>
      <th>Precio_Unitario</th>
      <th>Total_Factura</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>N-1000</td>
      <td>2023-05-10 03:00:00</td>
      <td>Usuario_0</td>
      <td>Gran Vía, 49, Piso 4</td>
      <td>Laptop Gamer</td>
      <td>1</td>
      <td>970</td>
      <td>$970.00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>N-1001</td>
      <td>2023-06-18 09:00:00</td>
      <td>Usuario_1</td>
      <td>Av. Libertad, 30, Piso 4</td>
      <td>Mouse Ergonómico</td>
      <td>4</td>
      <td>927</td>
      <td>$3708.00</td>
    </tr>
    <tr>
      <th>2</th>
      <td>N-1002</td>
      <td>2023-03-10 05:00:00</td>
      <td>Usuario_2</td>
      <td>Av. Libertad, 98, Piso 2</td>
      <td>Monitor 4K</td>
      <td>2</td>
      <td>1410</td>
      <td>$2820.00</td>
    </tr>
    <tr>
      <th>3</th>
      <td>N-1003</td>
      <td>2023-05-10 03:00:00</td>
      <td>Usuario_3</td>
      <td>Paseo Gracia, 94, Piso 7</td>
      <td>Webcam HD</td>
      <td>2</td>
      <td>1269</td>
      <td>$2538.00</td>
    </tr>
    <tr>
      <th>4</th>
      <td>N-1004</td>
      <td>2023-02-25 17:00:00</td>
      <td>Usuario_4</td>
      <td>Calle Mayor, 80, Piso 8</td>
      <td>Webcam HD</td>
      <td>2</td>
      <td>454</td>
      <td>$908.00</td>
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
      <th>145</th>
      <td>N-1145</td>
      <td>2023-04-17 10:00:00</td>
      <td>Usuario_145</td>
      <td>Paseo Gracia, 99, Piso 10</td>
      <td>Teclado Mecánico</td>
      <td>4</td>
      <td>1051</td>
      <td>$4204.00</td>
    </tr>
    <tr>
      <th>146</th>
      <td>N-1146</td>
      <td>2023-04-08 00:00:00</td>
      <td>Usuario_146</td>
      <td>Paseo Gracia, 64, Piso 4</td>
      <td>Monitor 4K</td>
      <td>2</td>
      <td>1229</td>
      <td>$2458.00</td>
    </tr>
    <tr>
      <th>147</th>
      <td>N-1147</td>
      <td>2023-02-01 22:00:00</td>
      <td>Usuario_147</td>
      <td>Calle Mayor, 81, Piso 1</td>
      <td>Monitor 4K</td>
      <td>1</td>
      <td>1040</td>
      <td>$1040.00</td>
    </tr>
    <tr>
      <th>148</th>
      <td>N-1148</td>
      <td>2023-06-13 03:00:00</td>
      <td>Usuario_148</td>
      <td>Calle Mayor, 56, Piso 1</td>
      <td>Laptop Gamer</td>
      <td>1</td>
      <td>297</td>
      <td>$297.00</td>
    </tr>
    <tr>
      <th>149</th>
      <td>N-1149</td>
      <td>2023-02-24 01:00:00</td>
      <td>Usuario_149</td>
      <td>Paseo Gracia, 25, Piso 1</td>
      <td>Monitor 4K</td>
      <td>1</td>
      <td>909</td>
      <td>$909.00</td>
    </tr>
  </tbody>
</table>
<p>150 rows × 8 columns</p>
</div>




```python
df_norte["Total_Factura"] = (
    df_norte["Total_Factura"]
        .str.replace("$","")
        .astype(float)
)
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
      <th>ID_Pedido</th>
      <th>Fecha_Transaccion</th>
      <th>Cliente_Nombre</th>
      <th>Direccion_Envio</th>
      <th>Producto</th>
      <th>Unidades</th>
      <th>Precio_Unitario</th>
      <th>Total_Factura</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>N-1000</td>
      <td>2023-05-10 03:00:00</td>
      <td>Usuario_0</td>
      <td>Gran Vía, 49, Piso 4</td>
      <td>Laptop Gamer</td>
      <td>1</td>
      <td>970</td>
      <td>970.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>N-1001</td>
      <td>2023-06-18 09:00:00</td>
      <td>Usuario_1</td>
      <td>Av. Libertad, 30, Piso 4</td>
      <td>Mouse Ergonómico</td>
      <td>4</td>
      <td>927</td>
      <td>3708.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>N-1002</td>
      <td>2023-03-10 05:00:00</td>
      <td>Usuario_2</td>
      <td>Av. Libertad, 98, Piso 2</td>
      <td>Monitor 4K</td>
      <td>2</td>
      <td>1410</td>
      <td>2820.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>N-1003</td>
      <td>2023-05-10 03:00:00</td>
      <td>Usuario_3</td>
      <td>Paseo Gracia, 94, Piso 7</td>
      <td>Webcam HD</td>
      <td>2</td>
      <td>1269</td>
      <td>2538.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>N-1004</td>
      <td>2023-02-25 17:00:00</td>
      <td>Usuario_4</td>
      <td>Calle Mayor, 80, Piso 8</td>
      <td>Webcam HD</td>
      <td>2</td>
      <td>454</td>
      <td>908.0</td>
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
      <th>145</th>
      <td>N-1145</td>
      <td>2023-04-17 10:00:00</td>
      <td>Usuario_145</td>
      <td>Paseo Gracia, 99, Piso 10</td>
      <td>Teclado Mecánico</td>
      <td>4</td>
      <td>1051</td>
      <td>4204.0</td>
    </tr>
    <tr>
      <th>146</th>
      <td>N-1146</td>
      <td>2023-04-08 00:00:00</td>
      <td>Usuario_146</td>
      <td>Paseo Gracia, 64, Piso 4</td>
      <td>Monitor 4K</td>
      <td>2</td>
      <td>1229</td>
      <td>2458.0</td>
    </tr>
    <tr>
      <th>147</th>
      <td>N-1147</td>
      <td>2023-02-01 22:00:00</td>
      <td>Usuario_147</td>
      <td>Calle Mayor, 81, Piso 1</td>
      <td>Monitor 4K</td>
      <td>1</td>
      <td>1040</td>
      <td>1040.0</td>
    </tr>
    <tr>
      <th>148</th>
      <td>N-1148</td>
      <td>2023-06-13 03:00:00</td>
      <td>Usuario_148</td>
      <td>Calle Mayor, 56, Piso 1</td>
      <td>Laptop Gamer</td>
      <td>1</td>
      <td>297</td>
      <td>297.0</td>
    </tr>
    <tr>
      <th>149</th>
      <td>N-1149</td>
      <td>2023-02-24 01:00:00</td>
      <td>Usuario_149</td>
      <td>Paseo Gracia, 25, Piso 1</td>
      <td>Monitor 4K</td>
      <td>1</td>
      <td>909</td>
      <td>909.0</td>
    </tr>
  </tbody>
</table>
<p>150 rows × 8 columns</p>
</div>




```python
# Sur
hojas_dict = pd.read_excel('data/ventas_sur_v2.xlsx', sheet_name=None)
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
      <th>Ref_Venta</th>
      <th>Fecha_Alta</th>
      <th>Articulo</th>
      <th>Cantidad</th>
      <th>Precio_Base</th>
      <th>Descuento_Aplicado</th>
      <th>Es_Cliente_Corporativo</th>
      <th>Estado_Envio</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>S-5000</td>
      <td>2023-02-11 00:00:00</td>
      <td>Webcam HD</td>
      <td>9</td>
      <td>425.12</td>
      <td>0.20</td>
      <td>False</td>
      <td>Enviado</td>
    </tr>
    <tr>
      <th>1</th>
      <td>S-5001</td>
      <td>2023-02-03 12:00:00</td>
      <td>Teclado Mecánico</td>
      <td>8</td>
      <td>599.60</td>
      <td>0.00</td>
      <td>True</td>
      <td>Devuelto</td>
    </tr>
    <tr>
      <th>2</th>
      <td>S-5002</td>
      <td>2023-04-06 07:00:00</td>
      <td>Mouse Ergonómico</td>
      <td>6</td>
      <td>971.36</td>
      <td>0.00</td>
      <td>False</td>
      <td>Completado</td>
    </tr>
    <tr>
      <th>3</th>
      <td>S-5003</td>
      <td>2023-04-02 05:00:00</td>
      <td>Monitor 4K</td>
      <td>1</td>
      <td>799.66</td>
      <td>0.10</td>
      <td>True</td>
      <td>Pendiente</td>
    </tr>
    <tr>
      <th>4</th>
      <td>S-5004</td>
      <td>2023-06-17 12:00:00</td>
      <td>Mouse Ergonómico</td>
      <td>2</td>
      <td>619.99</td>
      <td>0.05</td>
      <td>True</td>
      <td>Devuelto</td>
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
      <th>155</th>
      <td>S-5075</td>
      <td>2023-05-17 22:00:00</td>
      <td>Monitor 4K</td>
      <td>2</td>
      <td>1150.01</td>
      <td>0.10</td>
      <td>False</td>
      <td>Enviado</td>
    </tr>
    <tr>
      <th>156</th>
      <td>S-5076</td>
      <td>2023-01-29 13:00:00</td>
      <td>Webcam HD</td>
      <td>1</td>
      <td>874.12</td>
      <td>0.10</td>
      <td>True</td>
      <td>Devuelto</td>
    </tr>
    <tr>
      <th>157</th>
      <td>S-5077</td>
      <td>2023-06-09 11:00:00</td>
      <td>Mouse Ergonómico</td>
      <td>1</td>
      <td>145.92</td>
      <td>0.00</td>
      <td>True</td>
      <td>Devuelto</td>
    </tr>
    <tr>
      <th>158</th>
      <td>S-5078</td>
      <td>2023-02-07 13:00:00</td>
      <td>Monitor 4K</td>
      <td>2</td>
      <td>1152.24</td>
      <td>0.00</td>
      <td>False</td>
      <td>Enviado</td>
    </tr>
    <tr>
      <th>159</th>
      <td>S-5079</td>
      <td>2023-05-01 11:00:00</td>
      <td>Docking Station</td>
      <td>7</td>
      <td>261.88</td>
      <td>0.10</td>
      <td>True</td>
      <td>Enviado</td>
    </tr>
  </tbody>
</table>
<p>160 rows × 8 columns</p>
</div>




```python
df_sur["Total"] = (
    df_sur["Precio_Base"] * 
    df_sur["Cantidad"] * 
    (1 - df_sur["Descuento_Aplicado"])
)
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
      <th>Ref_Venta</th>
      <th>Fecha_Alta</th>
      <th>Articulo</th>
      <th>Cantidad</th>
      <th>Precio_Base</th>
      <th>Descuento_Aplicado</th>
      <th>Es_Cliente_Corporativo</th>
      <th>Estado_Envio</th>
      <th>Total</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>S-5000</td>
      <td>2023-02-11 00:00:00</td>
      <td>Webcam HD</td>
      <td>9</td>
      <td>425.12</td>
      <td>0.20</td>
      <td>False</td>
      <td>Enviado</td>
      <td>3060.864</td>
    </tr>
    <tr>
      <th>1</th>
      <td>S-5001</td>
      <td>2023-02-03 12:00:00</td>
      <td>Teclado Mecánico</td>
      <td>8</td>
      <td>599.60</td>
      <td>0.00</td>
      <td>True</td>
      <td>Devuelto</td>
      <td>4796.800</td>
    </tr>
    <tr>
      <th>2</th>
      <td>S-5002</td>
      <td>2023-04-06 07:00:00</td>
      <td>Mouse Ergonómico</td>
      <td>6</td>
      <td>971.36</td>
      <td>0.00</td>
      <td>False</td>
      <td>Completado</td>
      <td>5828.160</td>
    </tr>
    <tr>
      <th>3</th>
      <td>S-5003</td>
      <td>2023-04-02 05:00:00</td>
      <td>Monitor 4K</td>
      <td>1</td>
      <td>799.66</td>
      <td>0.10</td>
      <td>True</td>
      <td>Pendiente</td>
      <td>719.694</td>
    </tr>
    <tr>
      <th>4</th>
      <td>S-5004</td>
      <td>2023-06-17 12:00:00</td>
      <td>Mouse Ergonómico</td>
      <td>2</td>
      <td>619.99</td>
      <td>0.05</td>
      <td>True</td>
      <td>Devuelto</td>
      <td>1177.981</td>
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
      <td>...</td>
    </tr>
    <tr>
      <th>155</th>
      <td>S-5075</td>
      <td>2023-05-17 22:00:00</td>
      <td>Monitor 4K</td>
      <td>2</td>
      <td>1150.01</td>
      <td>0.10</td>
      <td>False</td>
      <td>Enviado</td>
      <td>2070.018</td>
    </tr>
    <tr>
      <th>156</th>
      <td>S-5076</td>
      <td>2023-01-29 13:00:00</td>
      <td>Webcam HD</td>
      <td>1</td>
      <td>874.12</td>
      <td>0.10</td>
      <td>True</td>
      <td>Devuelto</td>
      <td>786.708</td>
    </tr>
    <tr>
      <th>157</th>
      <td>S-5077</td>
      <td>2023-06-09 11:00:00</td>
      <td>Mouse Ergonómico</td>
      <td>1</td>
      <td>145.92</td>
      <td>0.00</td>
      <td>True</td>
      <td>Devuelto</td>
      <td>145.920</td>
    </tr>
    <tr>
      <th>158</th>
      <td>S-5078</td>
      <td>2023-02-07 13:00:00</td>
      <td>Monitor 4K</td>
      <td>2</td>
      <td>1152.24</td>
      <td>0.00</td>
      <td>False</td>
      <td>Enviado</td>
      <td>2304.480</td>
    </tr>
    <tr>
      <th>159</th>
      <td>S-5079</td>
      <td>2023-05-01 11:00:00</td>
      <td>Docking Station</td>
      <td>7</td>
      <td>261.88</td>
      <td>0.10</td>
      <td>True</td>
      <td>Enviado</td>
      <td>1649.844</td>
    </tr>
  </tbody>
</table>
<p>160 rows × 9 columns</p>
</div>




```python
# Este
with open("data/ventas_este_v2.json") as f:
    data = json.load(f)

registros = []

for item in data:
    data = item["data"]
    payload = data["payload"]
    producto = payload["transaccion"]["detalles_producto"]

    registros.append({
        "ID": data["id_registro"],
        "Fecha": payload["fecha_evento"],
        "Ciudad": payload["comprador"]["ubicacion"]["ciudad"],
        "Producto": producto["nombre_comercial"],
        "Cantidad": payload["transaccion"]["cantidad_comprada"],
        "Precio_Lista": producto["precio_lista"],
        "IVA": producto["impuestos"]["monto_iva"]
    })

df_este = pd.DataFrame(registros)

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
      <th>ID</th>
      <th>Fecha</th>
      <th>Ciudad</th>
      <th>Producto</th>
      <th>Cantidad</th>
      <th>Precio_Lista</th>
      <th>IVA</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>E-8000</td>
      <td>2023-06-29 00:00:00</td>
      <td>Sevilla</td>
      <td>Monitor 4K</td>
      <td>1</td>
      <td>503</td>
      <td>105.63</td>
    </tr>
    <tr>
      <th>1</th>
      <td>E-8001</td>
      <td>2023-04-12 00:00:00</td>
      <td>Sevilla</td>
      <td>Webcam HD</td>
      <td>1</td>
      <td>1122</td>
      <td>235.62</td>
    </tr>
    <tr>
      <th>2</th>
      <td>E-8002</td>
      <td>2023-04-20 00:00:00</td>
      <td>Sevilla</td>
      <td>Webcam HD</td>
      <td>2</td>
      <td>994</td>
      <td>208.74</td>
    </tr>
    <tr>
      <th>3</th>
      <td>E-8003</td>
      <td>2023-04-14 00:00:00</td>
      <td>Madrid</td>
      <td>Webcam HD</td>
      <td>2</td>
      <td>1255</td>
      <td>263.55</td>
    </tr>
    <tr>
      <th>4</th>
      <td>E-8004</td>
      <td>2023-01-03 00:00:00</td>
      <td>Sevilla</td>
      <td>Monitor 4K</td>
      <td>2</td>
      <td>1458</td>
      <td>306.18</td>
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
    </tr>
    <tr>
      <th>115</th>
      <td>E-8115</td>
      <td>2023-04-05 00:00:00</td>
      <td>Valencia</td>
      <td>Monitor 4K</td>
      <td>1</td>
      <td>829</td>
      <td>174.09</td>
    </tr>
    <tr>
      <th>116</th>
      <td>E-8116</td>
      <td>2023-04-23 00:00:00</td>
      <td>Valencia</td>
      <td>Laptop Gamer</td>
      <td>2</td>
      <td>858</td>
      <td>180.18</td>
    </tr>
    <tr>
      <th>117</th>
      <td>E-8117</td>
      <td>2023-06-03 00:00:00</td>
      <td>Sevilla</td>
      <td>Webcam HD</td>
      <td>2</td>
      <td>637</td>
      <td>133.77</td>
    </tr>
    <tr>
      <th>118</th>
      <td>E-8118</td>
      <td>2023-02-24 00:00:00</td>
      <td>Valencia</td>
      <td>Monitor 4K</td>
      <td>2</td>
      <td>502</td>
      <td>105.42</td>
    </tr>
    <tr>
      <th>119</th>
      <td>E-8119</td>
      <td>2023-01-08 00:00:00</td>
      <td>Madrid</td>
      <td>Mouse Ergonómico</td>
      <td>1</td>
      <td>306</td>
      <td>64.26</td>
    </tr>
  </tbody>
</table>
<p>120 rows × 7 columns</p>
</div>




```python
df_norte_final = df_norte.rename(columns={
    "ID_Pedido": "id_transaccion",
    "Fecha_Transaccion": "fecha",
    "Producto": "producto",
    "Unidades": "cantidad",
    "Total_Factura": "total_venta"
})

df_norte_final["region"] = "Norte"
df_norte_final["ciudad"] = "No especificada"

df_norte_final = df_norte_final[
    ["id_transaccion", "fecha", "region", "producto", "cantidad", "total_venta", "ciudad"]
]

df_norte_final
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
      <th>id_transaccion</th>
      <th>fecha</th>
      <th>region</th>
      <th>producto</th>
      <th>cantidad</th>
      <th>total_venta</th>
      <th>ciudad</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>N-1000</td>
      <td>2023-05-10 03:00:00</td>
      <td>Norte</td>
      <td>Laptop Gamer</td>
      <td>1</td>
      <td>970.0</td>
      <td>No especificada</td>
    </tr>
    <tr>
      <th>1</th>
      <td>N-1001</td>
      <td>2023-06-18 09:00:00</td>
      <td>Norte</td>
      <td>Mouse Ergonómico</td>
      <td>4</td>
      <td>3708.0</td>
      <td>No especificada</td>
    </tr>
    <tr>
      <th>2</th>
      <td>N-1002</td>
      <td>2023-03-10 05:00:00</td>
      <td>Norte</td>
      <td>Monitor 4K</td>
      <td>2</td>
      <td>2820.0</td>
      <td>No especificada</td>
    </tr>
    <tr>
      <th>3</th>
      <td>N-1003</td>
      <td>2023-05-10 03:00:00</td>
      <td>Norte</td>
      <td>Webcam HD</td>
      <td>2</td>
      <td>2538.0</td>
      <td>No especificada</td>
    </tr>
    <tr>
      <th>4</th>
      <td>N-1004</td>
      <td>2023-02-25 17:00:00</td>
      <td>Norte</td>
      <td>Webcam HD</td>
      <td>2</td>
      <td>908.0</td>
      <td>No especificada</td>
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
    </tr>
    <tr>
      <th>145</th>
      <td>N-1145</td>
      <td>2023-04-17 10:00:00</td>
      <td>Norte</td>
      <td>Teclado Mecánico</td>
      <td>4</td>
      <td>4204.0</td>
      <td>No especificada</td>
    </tr>
    <tr>
      <th>146</th>
      <td>N-1146</td>
      <td>2023-04-08 00:00:00</td>
      <td>Norte</td>
      <td>Monitor 4K</td>
      <td>2</td>
      <td>2458.0</td>
      <td>No especificada</td>
    </tr>
    <tr>
      <th>147</th>
      <td>N-1147</td>
      <td>2023-02-01 22:00:00</td>
      <td>Norte</td>
      <td>Monitor 4K</td>
      <td>1</td>
      <td>1040.0</td>
      <td>No especificada</td>
    </tr>
    <tr>
      <th>148</th>
      <td>N-1148</td>
      <td>2023-06-13 03:00:00</td>
      <td>Norte</td>
      <td>Laptop Gamer</td>
      <td>1</td>
      <td>297.0</td>
      <td>No especificada</td>
    </tr>
    <tr>
      <th>149</th>
      <td>N-1149</td>
      <td>2023-02-24 01:00:00</td>
      <td>Norte</td>
      <td>Monitor 4K</td>
      <td>1</td>
      <td>909.0</td>
      <td>No especificada</td>
    </tr>
  </tbody>
</table>
<p>150 rows × 7 columns</p>
</div>




```python
df_sur_final = df_sur.rename(columns={
    "Ref_Venta": "id_transaccion",
    "Fecha_Alta": "fecha",
    "Articulo": "producto",
    "Cantidad": "cantidad",
    "Total": "total_venta"
})

df_sur_final["region"] = "Sur"
df_sur_final["ciudad"] = "No especificada"

df_sur_final = df_sur_final[
    ["id_transaccion", "fecha", "region", "producto", "cantidad", "total_venta", "ciudad"]
]

df_sur_final
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
      <th>id_transaccion</th>
      <th>fecha</th>
      <th>region</th>
      <th>producto</th>
      <th>cantidad</th>
      <th>total_venta</th>
      <th>ciudad</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>S-5000</td>
      <td>2023-02-11 00:00:00</td>
      <td>Sur</td>
      <td>Webcam HD</td>
      <td>9</td>
      <td>3060.864</td>
      <td>No especificada</td>
    </tr>
    <tr>
      <th>1</th>
      <td>S-5001</td>
      <td>2023-02-03 12:00:00</td>
      <td>Sur</td>
      <td>Teclado Mecánico</td>
      <td>8</td>
      <td>4796.800</td>
      <td>No especificada</td>
    </tr>
    <tr>
      <th>2</th>
      <td>S-5002</td>
      <td>2023-04-06 07:00:00</td>
      <td>Sur</td>
      <td>Mouse Ergonómico</td>
      <td>6</td>
      <td>5828.160</td>
      <td>No especificada</td>
    </tr>
    <tr>
      <th>3</th>
      <td>S-5003</td>
      <td>2023-04-02 05:00:00</td>
      <td>Sur</td>
      <td>Monitor 4K</td>
      <td>1</td>
      <td>719.694</td>
      <td>No especificada</td>
    </tr>
    <tr>
      <th>4</th>
      <td>S-5004</td>
      <td>2023-06-17 12:00:00</td>
      <td>Sur</td>
      <td>Mouse Ergonómico</td>
      <td>2</td>
      <td>1177.981</td>
      <td>No especificada</td>
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
    </tr>
    <tr>
      <th>155</th>
      <td>S-5075</td>
      <td>2023-05-17 22:00:00</td>
      <td>Sur</td>
      <td>Monitor 4K</td>
      <td>2</td>
      <td>2070.018</td>
      <td>No especificada</td>
    </tr>
    <tr>
      <th>156</th>
      <td>S-5076</td>
      <td>2023-01-29 13:00:00</td>
      <td>Sur</td>
      <td>Webcam HD</td>
      <td>1</td>
      <td>786.708</td>
      <td>No especificada</td>
    </tr>
    <tr>
      <th>157</th>
      <td>S-5077</td>
      <td>2023-06-09 11:00:00</td>
      <td>Sur</td>
      <td>Mouse Ergonómico</td>
      <td>1</td>
      <td>145.920</td>
      <td>No especificada</td>
    </tr>
    <tr>
      <th>158</th>
      <td>S-5078</td>
      <td>2023-02-07 13:00:00</td>
      <td>Sur</td>
      <td>Monitor 4K</td>
      <td>2</td>
      <td>2304.480</td>
      <td>No especificada</td>
    </tr>
    <tr>
      <th>159</th>
      <td>S-5079</td>
      <td>2023-05-01 11:00:00</td>
      <td>Sur</td>
      <td>Docking Station</td>
      <td>7</td>
      <td>1649.844</td>
      <td>No especificada</td>
    </tr>
  </tbody>
</table>
<p>160 rows × 7 columns</p>
</div>




```python
df_este_final = df_este.rename(columns={
    "ID": "id_transaccion",
    "Fecha": "fecha",
    "Producto": "producto",
    "Cantidad": "cantidad",
    "Precio_Lista": "total_venta",
    "Ciudad": "ciudad"
})

df_este_final["region"] = "Este"

df_este_final = df_este_final[
    ["id_transaccion", "fecha", "region", "producto", "cantidad", "total_venta", "ciudad"]
]

df_este_final
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
      <th>id_transaccion</th>
      <th>fecha</th>
      <th>region</th>
      <th>producto</th>
      <th>cantidad</th>
      <th>total_venta</th>
      <th>ciudad</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>E-8000</td>
      <td>2023-06-29 00:00:00</td>
      <td>Este</td>
      <td>Monitor 4K</td>
      <td>1</td>
      <td>503</td>
      <td>Sevilla</td>
    </tr>
    <tr>
      <th>1</th>
      <td>E-8001</td>
      <td>2023-04-12 00:00:00</td>
      <td>Este</td>
      <td>Webcam HD</td>
      <td>1</td>
      <td>1122</td>
      <td>Sevilla</td>
    </tr>
    <tr>
      <th>2</th>
      <td>E-8002</td>
      <td>2023-04-20 00:00:00</td>
      <td>Este</td>
      <td>Webcam HD</td>
      <td>2</td>
      <td>994</td>
      <td>Sevilla</td>
    </tr>
    <tr>
      <th>3</th>
      <td>E-8003</td>
      <td>2023-04-14 00:00:00</td>
      <td>Este</td>
      <td>Webcam HD</td>
      <td>2</td>
      <td>1255</td>
      <td>Madrid</td>
    </tr>
    <tr>
      <th>4</th>
      <td>E-8004</td>
      <td>2023-01-03 00:00:00</td>
      <td>Este</td>
      <td>Monitor 4K</td>
      <td>2</td>
      <td>1458</td>
      <td>Sevilla</td>
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
    </tr>
    <tr>
      <th>115</th>
      <td>E-8115</td>
      <td>2023-04-05 00:00:00</td>
      <td>Este</td>
      <td>Monitor 4K</td>
      <td>1</td>
      <td>829</td>
      <td>Valencia</td>
    </tr>
    <tr>
      <th>116</th>
      <td>E-8116</td>
      <td>2023-04-23 00:00:00</td>
      <td>Este</td>
      <td>Laptop Gamer</td>
      <td>2</td>
      <td>858</td>
      <td>Valencia</td>
    </tr>
    <tr>
      <th>117</th>
      <td>E-8117</td>
      <td>2023-06-03 00:00:00</td>
      <td>Este</td>
      <td>Webcam HD</td>
      <td>2</td>
      <td>637</td>
      <td>Sevilla</td>
    </tr>
    <tr>
      <th>118</th>
      <td>E-8118</td>
      <td>2023-02-24 00:00:00</td>
      <td>Este</td>
      <td>Monitor 4K</td>
      <td>2</td>
      <td>502</td>
      <td>Valencia</td>
    </tr>
    <tr>
      <th>119</th>
      <td>E-8119</td>
      <td>2023-01-08 00:00:00</td>
      <td>Este</td>
      <td>Mouse Ergonómico</td>
      <td>1</td>
      <td>306</td>
      <td>Madrid</td>
    </tr>
  </tbody>
</table>
<p>120 rows × 7 columns</p>
</div>




```python
df_total = pd.concat([df_norte_final, df_sur_final, df_este_final])
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
      <th>id_transaccion</th>
      <th>fecha</th>
      <th>region</th>
      <th>producto</th>
      <th>cantidad</th>
      <th>total_venta</th>
      <th>ciudad</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>N-1000</td>
      <td>2023-05-10 03:00:00</td>
      <td>Norte</td>
      <td>Laptop Gamer</td>
      <td>1</td>
      <td>970.0</td>
      <td>No especificada</td>
    </tr>
    <tr>
      <th>1</th>
      <td>N-1001</td>
      <td>2023-06-18 09:00:00</td>
      <td>Norte</td>
      <td>Mouse Ergonómico</td>
      <td>4</td>
      <td>3708.0</td>
      <td>No especificada</td>
    </tr>
    <tr>
      <th>2</th>
      <td>N-1002</td>
      <td>2023-03-10 05:00:00</td>
      <td>Norte</td>
      <td>Monitor 4K</td>
      <td>2</td>
      <td>2820.0</td>
      <td>No especificada</td>
    </tr>
    <tr>
      <th>3</th>
      <td>N-1003</td>
      <td>2023-05-10 03:00:00</td>
      <td>Norte</td>
      <td>Webcam HD</td>
      <td>2</td>
      <td>2538.0</td>
      <td>No especificada</td>
    </tr>
    <tr>
      <th>4</th>
      <td>N-1004</td>
      <td>2023-02-25 17:00:00</td>
      <td>Norte</td>
      <td>Webcam HD</td>
      <td>2</td>
      <td>908.0</td>
      <td>No especificada</td>
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
    </tr>
    <tr>
      <th>115</th>
      <td>E-8115</td>
      <td>2023-04-05 00:00:00</td>
      <td>Este</td>
      <td>Monitor 4K</td>
      <td>1</td>
      <td>829.0</td>
      <td>Valencia</td>
    </tr>
    <tr>
      <th>116</th>
      <td>E-8116</td>
      <td>2023-04-23 00:00:00</td>
      <td>Este</td>
      <td>Laptop Gamer</td>
      <td>2</td>
      <td>858.0</td>
      <td>Valencia</td>
    </tr>
    <tr>
      <th>117</th>
      <td>E-8117</td>
      <td>2023-06-03 00:00:00</td>
      <td>Este</td>
      <td>Webcam HD</td>
      <td>2</td>
      <td>637.0</td>
      <td>Sevilla</td>
    </tr>
    <tr>
      <th>118</th>
      <td>E-8118</td>
      <td>2023-02-24 00:00:00</td>
      <td>Este</td>
      <td>Monitor 4K</td>
      <td>2</td>
      <td>502.0</td>
      <td>Valencia</td>
    </tr>
    <tr>
      <th>119</th>
      <td>E-8119</td>
      <td>2023-01-08 00:00:00</td>
      <td>Este</td>
      <td>Mouse Ergonómico</td>
      <td>1</td>
      <td>306.0</td>
      <td>Madrid</td>
    </tr>
  </tbody>
</table>
<p>430 rows × 7 columns</p>
</div>


