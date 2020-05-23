import storm
import numpy as np
from json_tricks import loads

import gdal
import osr
import pickle

class CalcCoordinatesBoltPython(storm.BasicBolt):
	def initialize(self, conf, context):
		self._conf = conf
		self._context = context
		
	def process(self, tuple):
		nome_arquivo = tuple.values[0]
		projection_ref = tuple.values[1]
		xOrigin = float(tuple.values[2])
		yOrigin = float(tuple.values[3])
		pXh = float(tuple.values[4])
		pXw = float(tuple.values[5])
		pX = float(tuple.values[6])
		pY = float(tuple.values[7])
		
		mX = pX * pXw + xOrigin
		mY = pY * pXh + yOrigin

		crs = osr.SpatialReference()
		crs.ImportFromWkt(projection_ref)
		
		crsGeo = osr.SpatialReference()
		crsGeo.ImportFromEPSG(4326)
		t = osr.CoordinateTransformation(crs, crsGeo)
		coords = t.TransformPoint(mX, mY)

		latitude = coords[1]
		longitude = coords[0]

		storm.emit([nome_arquivo, str(latitude), str(longitude)])

CalcCoordinatesBoltPython().run()