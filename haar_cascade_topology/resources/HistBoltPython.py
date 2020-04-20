import storm
import numpy as np
from json_tricks import loads

class HistBoltPython(storm.BasicBolt):
	def initialize(self, conf, context):
		self._conf = conf
		self._context = context
		self._limit = 0.7
	
	def process(self, tuple):
		histValido = True
		msg = tuple.values[4]
		aux = loads(msg)
		aux = dict(aux)
		
		imagem = loads(aux['imagem'])
		img = np.array(imagem)

		hist = dict()
		x = img.shape[1]
		y = img.shape[0]

		for i in range(0,x):
			for j in range(0,y):
				if img[i,j] not in hist:
					hist[img[i,j]] = 1
				else:
					hist[img[i,j]] += 1

		total = img.shape[0]*img.shape[1]

		for key in sorted(hist):
			percent = hist[key]/total
			if percent >= self._limit:
				histValido = False

		if(histValido):
			nome_arquivo = aux['nome_arquivo']
			projection_ref = aux['projection_ref']
			x_origin = aux['x_origin']
			y_origin = aux['y_origin']
			px_h = aux['px_h']
			px_w = aux['px_w']
			X0 = aux['X0']
			Y0 = aux['Y0']
			storm.emit([nome_arquivo, aux['imagem'], projection_ref, str(x_origin), str(y_origin), str(px_h), str(px_w), str(X0), str(Y0)])

HistBoltPython().run()