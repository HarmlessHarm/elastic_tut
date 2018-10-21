from elasticsearch import Elasticsearch, helpers

from searchapp.constants import DOC_TYPE, INDEX_NAME
from searchapp.data import all_products, ProductData


def main():
	# Connect to localhost:9200 by default.
	es = Elasticsearch()

	es.indices.delete(index=INDEX_NAME, ignore=404)
	es.indices.create(
		index=INDEX_NAME,
		body={
			'mappings': {
				DOC_TYPE: {
					'properties': {
						'name': {
							'type': 'text',
							'fields': {
								'english_analyzed': {
									'type': 'text',
									'analyzer': 'custom_english_analyzer',
								},
							},
						},
						'description': {
							'type': 'text',
							'fields': {
								'english_analyzed': {
									'type': 'text',
									'analyzer': 'custom_english_analyzer',
								},
							},
						},
					},
				},
			},
			'settings': {
				'analysis': {
					'analyzer': {
						'custom_english_analyzer': {
							'type': 'english',
							'stopwords': ['made','_english_'],
						},
					},
				},
			},
		},
	)

	helpers.bulk(es, products_to_index(all_products()))
	# for product in all_products():
	#     index_product(es, product)


def index_product(es, product: ProductData):
	"""Add a single product to the ProductData index."""
	es.create(
		index=INDEX_NAME,
		doc_type=DOC_TYPE,
		id=product.id,
		body={
			"name": product.name,
			"image": product.image,
		}
	)

	# Don't delete this! You'll need it to see if your indexing job is working,
	# or if it has stalled.
	print("Indexed {}, {}".format(product.id, product.name))

def products_to_index(products):
	# helpers.bulk(es, products)
	for product in products:
		yield {
			"_op_type":'create',
			"_index":INDEX_NAME,
			"_type":DOC_TYPE,
			"_id":product.id,
			"_source": {
				"name":product.name,
				"description": product.description,
				"image":product.image
			}
		}
	


if __name__ == '__main__':
	main()
