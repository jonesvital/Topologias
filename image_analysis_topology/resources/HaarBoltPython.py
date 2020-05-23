import storm
import cv2
import numpy as np
from json_tricks import loads

class HaarBoltPython(storm.BasicBolt):
	def initialize(self, conf, context):
		self._conf = conf
		self._context = context
		#self._classificador = cv2.CascadeClassifier("/home/administrador/Documents/PUC/13 - TCC/Topologias/image_analysis_topology/resources/cascade_50_15_haar.xml")
		self._classificador = cv2.CascadeClassifier("/home/administrador/resources/cascade_50_15_haar.xml")

	def process(self, tuple):
		nome_arquivo = tuple.values[0]
		imagem = loads(tuple.values[1])
		projection_ref = tuple.values[2]
		xOrigin = tuple.values[3]
		yOrigin = tuple.values[4]
		pXh = tuple.values[5]
		pXw = tuple.values[6]
		X0 = int(tuple.values[7])
		Y0 = int(tuple.values[8])

		img = np.array(imagem)
		pistas = self._classificador.detectMultiScale(img, 1.3, 5, 1, (60, 60))

		for (x, y, w, h) in pistas:
			pX = X0 + x + round(w/2)
			pY = Y0 + y + round(h/2)
			storm.emit([nome_arquivo, projection_ref, xOrigin, yOrigin, pXh, pXw, str(pX), str(pY)])

HaarBoltPython().run()