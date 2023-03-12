const striptags = require('striptags');
const { flattenObject, objectMap, filteredObject } = require('./utils.js');
const availableIndexers = require('./indexers/index.js');

let initializedIndexes = [];

module.exports = createIndexer;

/**
 * @param {import('../types.js').ExtensionConfig} config
 * @param {any} context
 */
function createIndexer(config, { logger, database, services, getSchema }) {
	if (!config.server.type || !availableIndexers[config.server.type]) {
		throw Error(`Broken config file. Missing or invalid indexer type "${config.server.type || 'Unknown'}".`);
	}

	const indexer = availableIndexers[config.server.type](config.server);

	return {
		ensureCollectionIndex,
		initCollectionIndexes,

		initItemsIndex,
		updateItemIndex,
		deleteItemIndex,

		getCollectionIndexName,
	};

	/**
	 * @param {string} collection
	 */
	async function ensureCollectionIndex(collection) {
		const collectionIndex = getCollectionIndexName(collection);
		try {
			await indexer.createIndex(collectionIndex);
		} catch (error) {
			logger.warn(`Cannot create collection "${collectionIndex}". ${getErrorMessage(error)}`);
			logger.debug(error);
		}
	}

	async function initCollectionIndexes() {
		for (const collection of Object.keys(config.collections)) {
			await ensureCollectionIndex(collection);
			await initItemsIndex(collection);
		}
	}

	/**
	 * @param {string} collection
	 */
	async function initItemsIndex(collection) {
		const schema = await getSchema();

		if (!schema.collections[collection]) {
			logger.warn(`Collection "${collection}" does not exists.`);
			return;
		}

		const query = new services.ItemsService(collection, { database, schema });

		if (!initializedIndexes.includes(getCollectionIndexName(collection))) {
			try {
				await indexer.deleteItems(getCollectionIndexName(collection));
			} catch (error) {
				logger.warn(`Cannot drop collection "${collection}". ${getErrorMessage(error)}`);
				logger.debug(error);
			}

			try {
				await indexer.updateIndexSettings(getCollectionIndexName(collection), config.collections[collection].settings);
			} catch (error) {
				logger.warn(`Failed to set collection settings for "${collection}". ${getErrorMessage(error)}`);
				logger.debug(error);
			}

			initializedIndexes.push(getCollectionIndexName(collection));
		}

		const pk = schema.collections[collection].primary;
		const limit = config.batchLimit || 100;

		for (let offset = 0; ; offset += limit) {
			const items = await query.readByQuery({
				fields: [pk],
				filter: config.collections[collection].filter || [],
				limit,
				offset,
			});

			if (!items || !items.length) break;

			await updateItemIndex(
				collection,
				items.map((/** @type {{ [x: string]: any; }} */ i) => i[pk])
			);
		}
	}

	/**
	 * @param {string} collection
	 * @param {string[]} ids
	 */
	async function deleteItemIndex(collection, ids) {
		const schema = await getSchema();

		const collectionIndex = getCollectionIndexName(collection);

		const query = new services.ItemsService(collection, {
			knex: database,
			schema: schema,
		});

		const pk = schema.collections[collection].primary;

		const items = await query.readMany(ids, {
			fields: config.collections[collection].fields ? [pk, ...config.collections[collection].fields] : ['*'],
			filter: config.collections[collection].filter || [],
		});

		for (const item of items) {
			const id = getDocumentPkValue(item, collection, pk);

			try {
				await indexer.deleteItem(collectionIndex, id);
			} catch (error) {
				logger.warn(`Cannot delete "${collectionIndex}/${id}". ${getErrorMessage(error)}`);
				logger.debug(error);
			}
		}
	}

	/**
	 * @param {string} collection
	 * @param {string[]} ids
	 */
	async function updateItemIndex(collection, ids) {
		const schema = await getSchema();
		/**
		 * @type {Map<string, string[]>}
		 */
		let collectionsToUpdate = new Map();

		if (!config.collections.hasOwnProperty(collection)) {
			// Check if the collection is related to any configured collections
			collectionsToUpdate = await getRelatedCollections(schema, collection, ids);
		} else {
			collectionsToUpdate.set(collection, ids);
		}

		collectionsToUpdate.forEach(async (ids, collection) => {
			const collectionIndex = getCollectionIndexName(collection);

			const query = new services.ItemsService(collection, {
				knex: database,
				schema: schema,
			});

			const pk = schema.collections[collection].primary;

			const items = await query.readMany(ids, {
				fields: config.collections[collection].fields ? [pk, ...config.collections[collection].fields] : ['*'],
				filter: config.collections[collection].filter || [],
			});

			/**
			 * @type {string[]}
			 */
			const processedIds = [];

			for (const item of items) {
				const id = getDocumentPkValue(item, collection, pk);

				try {
					await indexer.updateItem(collectionIndex, id, prepareObject(item, collection), pk);

					processedIds.push(id);
				} catch (error) {
					logger.warn(`Cannot index "${collectionIndex}/${id}". ${getErrorMessage(error)}`);
					logger.debug(error);
				}
			}

			if (items.length < ids.length) {
				for (const id of ids.filter((x) => !processedIds.includes(x))) {
					const computedId = getDocumentPkValue({ [pk]: id }, collection, pk);

					try {
						await indexer.deleteItem(collectionIndex, computedId);
					} catch (error) {
						logger.warn(`Cannot index "${collectionIndex}/${computedId}". ${getErrorMessage(error)}`);
						logger.debug(error);
					}
				}
			}
		});
	}

	/**
	 * @param {object} body
	 * @param {string} collection
	 */
	function prepareObject(body, collection) {
		const meta = {};

		if (config.collections[collection].collectionField) {
			// @ts-ignore
			meta[config.collections[collection].collectionField] = collection;
		}

		if (config.collections[collection].transform) {
			return {
				// @ts-ignore
				...config.collections[collection].transform(
					body,
					{
						striptags,
						flattenObject,
						objectMap,
						filteredObject,
					},
					collection
				),
				...meta,
			};
		} else if (config.collections[collection].fields) {
			return {
				...filteredObject(flattenObject(body), config.collections[collection].fields),
				...meta,
			};
		}

		return {
			...body,
			...meta,
		};
	}

	/**
	 * @param {string} collection
	 * @returns {string}
	 */
	function getCollectionIndexName(collection) {
		return config.collections[collection].indexName || collection;
	}

	/**
	 *
	 * @param {object} item
	 * @param {string} collection
	 * @param {string} pk
	 * @returns {string}
	 */
	function getDocumentPkValue(item, collection, pk) {
		const callbackFn = config.collections[collection].computePk;

		if (callbackFn) return callbackFn(item, collection);

		return item[pk];
	}

	/**
	 * Gets all IDs of the configured collections which relate to the provided collection and its IDs.
	 * The function only checks up to one nesting level deep.
	 *
	 * @param {{relations: Record<string, any>[]}} schema
	 * @param {string} collection
	 * @param {string[]} ids
	 * @returns {Promise<Map<string, string[]>>}
	 */
	async function getRelatedCollections(schema, collection, ids) {
		/**
		 * @type {Map<string, string[]>}
		 */
		const relatedCollectionEntries = new Map();

		// Find all related collections which are configured as indexable collections
		const collectionRelations = schema.relations.filter((e) => {
			return e.related_collection === collection;
		});

		let configuredCollections = Object.keys(config.collections);

		for await (const relatedCollection of collectionRelations) {
			if (configuredCollections.includes(relatedCollection.collection)) {
				if (!relatedCollection.schema) continue; // No Knex foreign key schema provided, it's not possible to determine which documents of the collection need to be changed

				// Check if the column with the affected relation is used in the "fields" settings of the collection
				const fields = config.collections[relatedCollection.collection].fields;
				if (
					fields &&
					!fields.find((e) => {
						return e === relatedCollection.schema.column || e.startsWith(`${relatedCollection.schema.column}.`);
					})
				) {
					continue;
				}

				// Search for all fields in the configured collection which are affected by the change in the provided collection
				const query = new services.ItemsService(relatedCollection.collection, { database, schema });

				const affectedIds = await query.getKeysByQuery({
					filter: {
						[relatedCollection.schema.column]: {
							_in: ids,
						},
					},
				});

				if (relatedCollectionEntries.has(relatedCollection.collection)) {
					const previousValue = relatedCollectionEntries.get(relatedCollection.collection);

					relatedCollectionEntries.set(relatedCollection.collection, previousValue.concat(affectedIds));
				} else {
					relatedCollectionEntries.set(relatedCollection.collection, affectedIds);
				}
			}
		}

		return relatedCollectionEntries;
	}

	/**
	 * @param {any} error
	 * @returns {string}
	 */
	function getErrorMessage(error) {
		if (error && error.message) return error.message;

		if (error && error.response && error.response.data && error.response.data.error) return error.response.data.error;

		return error.toString();
	}
}
