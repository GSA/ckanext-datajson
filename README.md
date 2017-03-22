# ckanext-datajsonAR

La extensión de CKAN `datajsonAR`, fue confeccionada dentro del marco del plan de `Datos Abiertos` de la República Argentina e incluida dentro del proyecto [portal andino](https://github.com/datosgobar/portal-andino), en función de que el mismo pueda asi cumplir con estándares de calidad para la confección de metadata para catálogos de datos [DCAT](https://www.w3.org/TR/vocab-dcat) propuestos por la World Wide Web Consortium([W3C](www.w3c.es)). 
Este desarrollo es un fork del trabajo realizado por la [GSA](https://github.com/GSA), con la extensión para CKAN, [GSA/ckanext-datajson](https://github.com/GSA/ckanext-datajson)

Indice:
------
+ [Presentacion](#ckanext-datajsonar)
+ [Instalación](#instalación-de-la-extensión)
    + [Pre requisitos](#pre-requisitos)
    + [Proceso de instalación de la extensión datajsonAR](#proceso-de-instalación-de-la-extensión-datajsonar)
    + [Probado que todo funciona](#probado-que-todo-funciona)
+ [Usage](#usage)
+ [Tests](#tests)
+ [Credits / Copying](#credits--copying)
+ [Contacto](#contacto)
+ [Comentarios / preguntas](#comentarios-o-preguntas)



Instalación de la Extensión:
---

###### Pre requisitos:
+ Python 2.7.x 
+ CKAN 2.5.2
+ ckanext [gobAR-Theme](https://github.com/gobabiertoAR/portal-andino-theme)
+ ckanext [Hierarchy](https://github.com/datagovuk/ckanext-hierarchy)
+ ckanext [Harvest](https://github.com/ckan/ckanext-harvestt)

##### Proceso de instalación de la extensión datajsonAR
```bash
# Dentro del virtualenv de CKAN
# . ruta/al/ckan_pyenv/bin/activate
(ckan_pyenv) $ pip install pyyaml lepl jsonschema rfc3987
(ckan_pyenv) $ pip install -e "git+https://github.com/datosgobar/ckanext-datajson.git#egg=ckanext-datajson"
```
Dentro del apartado `[app:main]` de la configuracion de CKAN `(/etc/ckan/default/ckan_config.ini)` agregar los siguientes campos:
```bash
[app:main]
ckan.owner = entidad-responsable-del-portal # Admite espacios en blanco entre palabras
ckan.owner.email = mail@entidad-responsable-del-portal.xxx # si este campo no se define, esta info se tomara del area de "contacto" de GobAr-Theme
```
Luego, dentro de su archivo de configuracion de CKAN, normalmente alojado en `/etc/ckan/default/config_name.ini`, agregar la siguiente linea en la sección de `[plugins]` a `ckan.plugins`

	ckan.plugins = (otros plugins) datajson

Probado que todo funciona:
---
Para saber que el proceso de instalación se realizo con exito, solo debemos tipear, en un navegador:
	
	http://{tu_host}/data.json

Y deberíamos ver una respuesta similar a esta:
```JSON
{
    "title": "Título del portal",
    "description": "Descripcion del portal",
    "superThemeTaxonomy": "http://datos.gob.ar/superThemeTaxonomy.json",
    "publisher": {
        "mbox": "email@deContacto.xxx",
        "name": "Nombre de la persona o institución responsable de la instancia CKAN."
},
	"themeTaxonomy": [],
	"dataset": []
}
```


Usage:
---
Configurar la url de salida para nuestro `/data.json`, debebos agregar las sigientes lineas dentro del archivo de configuracion 
de CKAN`[/etc/ckan/default/ckan-config.ini]`

	ckanext.datajson.path = /data.json
	ckanext.datajsonld.id = http://www.tuagencia.gob.ar/data.json

Para habilitar o deshabilitar por configuracion la visibilidad del `/data.json`, sin tener que quitar el plugin, agregaremos dentro del archivo de configuracion de CKAN`[/etc/ckan/default/ckan-config.ini]` la siguiente sentencia:
	
	# False, no se mostrara http://{tu-host}/data.json
	# True, se muestra http://{tu-host}/data.json
    ckanext.datajson.url_enabled = False 



Tests:
---
*WIP*


Credits / Copying
---
El presente desarrollo es un fork del trabajo de la [GSA](link), [GSA/ckanext-datajson](https://github.com/GSA/ckanext-datajson) y podes visitarlo [Aquí](https://github.com/GSA/ckanext-datajson/blob/master/README.md) (EN).

Contacto:
---
Este proyecto es en desarrollo, si viste algun `bug`, por favor, [creanos un issue](https://github.com/datosgobar/ckanext-datajsonAR/issues/new?title=Encontre un bug!).

Comentarios o preguntas?
---
Escribinos a [datos@modernizacion.gob.ar](mailto:datos@modernizacion.gob.ar)
