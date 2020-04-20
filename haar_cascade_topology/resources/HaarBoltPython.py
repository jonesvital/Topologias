import storm
import cv2
import numpy as np
from json_tricks import loads

class HaarBoltPython(storm.BasicBolt):
	def initialize(self, conf, context):
		self._conf = conf
		self._context = context
		self._classificador = cv2.CascadeClassifier("/home/administrador/TCC-Puc/haar_cascade_topology/resources/cascade_50_15_haar.xml")
		# self._classificador = cv2.CascadeClassifier("/home/administrador/resources/cascade_50_15_haar.xml")

	def process(self, tuple):
		nome_arquivo = tuple.values[0]
		imagem = loads(tuple.values[1])
		projection_ref = tuple.values[2]
		x_origin = tuple.values[3]
		y_origin = tuple.values[4]
		px_h = tuple.values[5]
		px_w = tuple.values[6]
		X = int(tuple.values[7])
		Y = int(tuple.values[8])

		img = np.array(imagem)
		pistas = self._classificador.detectMultiScale(img, 1.3, 5, 1, (60, 60))

		for (x0, y0, w, h) in pistas:
			pX = X + x0 + round(w/2)
			pY = Y + y0 + round(h/2)
			storm.emit([nome_arquivo, projection_ref, x_origin, y_origin, px_h, px_w, str(pX), str(pY)])

HaarBoltPython().run()