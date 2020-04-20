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
		x_origin = float(tuple.values[2])
		y_origin = float(tuple.values[3])
		px_h = float(tuple.values[4])
		px_w = float(tuple.values[5])
		px = float(tuple.values[6])
		py = float(tuple.values[7])
		
		mX = px * px_w + x_origin
		mY = py * px_h + y_origin

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