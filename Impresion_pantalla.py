def table(tabla, opcion, orden, n): 

    #Ordenamos la lista de diccionarios según la opción deseada
    if opcion == 1:
      tabla.sort(key=(lambda x: x['estacion']))
    elif opcion == 2:
      tabla.sort(key=(lambda x: x['media']))
    elif opcion == 3:
      tabla.sort(key=(lambda x: x['usos']))
    elif opcion == 4:
      tabla.sort(key=(lambda x: x['riesgo_bajo']))
    elif opcion == 5:
      tabla.sort(key=(lambda x: x['riesgo_alto']))

    #Dependiendo del orden, tomamos n elementos de la lista ordenada o de la 
    #lista al revés y creamos un DataFrame para representarlo como tabla
    if orden == 1:
      tabla = tabla[0:n]
      result = pd.DataFrame(tabla)
    elif orden == 2:
      tabla.reverse()
      tabla = tabla[0:n]
      result = pd.DataFrame(tabla)

    #Hacemos un recuento de las estaciones utilizadas, para la gráfica
    #y el mapa
    lista = []
    for elem in tabla:
      lista.append(elem['estacion'])
      elem.pop('estacion') #las pondremos en el eje X al representar la gráfica
      elem.pop('media') #para la gráfica la media es demasiado pequeña en comparación con los demás valores

    print(tabulate(result, headers='keys', showindex=False, tablefmt='psql'))



    #Ahora sacamos el gráfico
    print(" ")
    print("El gráfico correspondiente a esta tabla es:")
    print(" ")
    result1 = pd.DataFrame(tabla, index=lista)
    result1.plot.bar()





#Funcion principal:




#Carga el archivo obtenido por el programa anterior y lo muestra en pantalla
#en forma de tabla (debes elegir el orden y el numero de paradas que quieres
#que aparezcan) y en forma de gráfica

def main(sc):
  print("Cargando lista_riesgo.json...")

  if os.path.isfile('/content/lista_riesgo.json'): #Buscamos el archivo
      print('Archivo cargado')
  else:
      print('ADVERTENCIA: El archivo no se ha encontrado.')
      print('Por favor, asegúrate de haber ejecutado el programa anterior.')
      sc.stop()
      sys.exit

  #Calcula el numero de elementos que tiene mediante una reduccion y forma una 
  #lista de diccionarios con las lineas de la RDD
  rdd_data = sc.textFile("/content/lista_riesgo.json")
  rdd_data = rdd_data.map(json.loads)
  numero_filas = rdd_data.map(lambda x: 1).reduce(lambda x,y: x + y)
  lista_tabla = rdd_data.collect()

  print("RDD cargada con éxito.")
  print("--------------------")
  print("Selecciona con respecto a qué valor deseas ordenar la tabla:")
  print(" ")
  print("1 -> Según el número de la estación")
  print("2 -> Según la media de puntuación")
  print("3 -> Según el número de usos por personas distintas de la estación")
  print("4 -> Según el número de usos por personas con puntuación menor que 0.5 (riesgo bajo)")
  print("5 -> Según el número de usos por personas con puntuación mayor que 0.5 (riesgo alto)")
  print(" ")

  opcion = input()

  #Se asegura de mostrar fallos de lectura si los carácteres introducidos no 
  #corresponden a ninguna opción
 
  i = 0
  while i != 1:
    if opcion not in ["1","2","3","4","5"]:
      print("No se reconoce el dígito marcado. Introduce una opcion de las anteriores")
      print(" ")
      opcion = input()
    else: 
      i = 1

  opcion = int(opcion)

  print(" ")
  print("Selecciona el orden según el cuál deseas ordenar la tabla:")
  print(" ")
  print("1 -> Orden creciente")
  print("2 -> Orden decreciente")
  print(" ")

  orden = input()


  i = 0
  while i != 1:
    if orden not in ["1","2"]:
      print("No se reconoce el dígito marcado. Por favor, introduzca una opción de las anteriores:")
      print(" ")
      orden = input()
    else: 
      i = 1

  orden = int(orden)

  print(" ")
  print("Selecciona el número de filas que quieres mostrar:")
  print("(Número de filas que desees mostrar) -> N primeras filas (máximo " + str(numero_filas) + ")")
  print(" ")

  numero = int(input())
  
  i = 0
  while i!= 1:
    if numero not in list(range(1, numero_filas)):
      print("El numero de filas seleccionado no existe")
      print("Por favor introduce un numero dentro del rango de filas")
      print("")
      numero = int(input())
    else:
      i = 1


  table(lista_tabla, opcion, orden, numero)


if __name__ == "__main__":
  sc = SparkContext()
  main(sc)
  sc.stop()