import {DataAssociationMapping, DataPermissionEventListener} from '@themost/data';
import {DataError} from '@themost/common';
import cloneDeep from 'lodash/cloneDeep';
import {eachSeries} from 'async';
import {QueryField} from '@themost/query';

/**
 * Listener for 'beforeExecute' event.
 * @param {import('@themost/data').DataEventArgs} event
 * @param {function(err?:Error)} callback
 */
function beforeExecute(event, callback) {
    if (typeof event.result !== 'undefined') {
        return callback();
    }
    if (event.emitter) {
        /**
         * @type {{emitter:import('./data-queryable').DataQueryable}}
         */
        const {emitter} = event;
        // Check if the emitter is a queryable object and if it has a select clause
        if (emitter.query && emitter.query.$select == null) {
            return callback();
        }
        // validate that formatter supports JSON array
        // noinspection JSUnresolvedReference
        if (typeof event.model.context.db.getFormatter !== 'function') {
            // exit without do nothing
            return callback();
        }
        // noinspection JSUnresolvedReference
        const formatter = event.model.context.db.getFormatter();
        if (formatter == null) {
            // exit without do nothing
            return callback();
        }
        // noinspection JSUnresolvedReference
        if (typeof formatter.$jsonGroupArray !== 'function') {
            // the formatter does not support JSON group array
            // exit without do nothing
            return callback();
        }
        const [selectView] = Object.keys(emitter.query.$select);
        const selectFields = emitter.query.$select[selectView];
        try {
            /**
             * @type {{expands:Array<*>, model:import('./data-model').DataModel}}
             */
            const {$expand, model} = emitter;
            /**
             * @type {{context: import('./types').DataContext}}
             */
            const { context } = model;
            if (Array.isArray($expand) && $expand.length > 0) {
                /**
                 * @type {Array<DataAssociationMapping>}
                 */
                const mappings = $expand.filter((expr) => {
                    return expr != null;
                }).map((expr) => {
                    if (expr instanceof DataAssociationMapping) {
                        // return copy
                        return cloneDeep(expr);
                    }
                    if (typeof expr === 'string') {
                        const mapping = model.inferMapping(expr);
                        if (mapping) {
                            // return copy
                            return cloneDeep(mapping);
                        }
                        throw new DataError('E_MAPPING', `Association mapping not found for ${model.name}.${expr}`, null, model.name, expr);
                    }
                    if (expr && expr.name) {
                        const mapping = cloneDeep(model.inferMapping(expr.name));
                        if (mapping) {
                            if (typeof expr.options === 'object') {
                                // merge options
                                return Object.assign(mapping.options, expr.options);
                            }
                            return mapping;
                        }
                        throw new DataError('E_MAPPING', `Association mapping not found for ${model.name}.${expr.name}`, null, model.name, expr.name);
                    }
                    throw new DataError('E_EXPR', 'Invalid association mapping expression. Expected a string or a valid data association mapping.', null, model.name);
                });
                if (mappings.length === 0) {
                    return callback();
                }
                const { viewAdapter: ModelView } = model;
                // iterate over expands and try to include them
                return eachSeries(mappings, (mapping, cb) => {
                    if (mapping.associationType === 'association' && mapping.childModel === model.name) {
                        // try to include json-like query for getting foreign key association
                        const options = mapping.options || {};
                        // get associated model query
                        const parentModel = context.model(mapping.parentModel);
                        // check if the parent model has the "@json.expandable" attribute enabled
                        const jsonExpandable = Object.getOwnPropertyDescriptor(parentModel, '@json.expandable');
                        if (jsonExpandable && jsonExpandable.value === true) {
                            // continue
                        } else {
                            return cb();
                        }
                        void parentModel.migrateAsync().then(() => {
                            void parentModel.filterAsync(options).then((q) => {
                                const { query } = q.prepare();
                                if (query.$select == null) {
                                    q.select();
                                }
                                const { viewAdapter: ParentView } = parentModel;
                                const fields = query.$select[ParentView] || [];
                                query.$select = {
                                    [ParentView]: [
                                        {
                                            value: {
                                                $jsonObject: fields
                                            }
                                        }
                                    ]
                                };
                                // pseudo-sql: WHERE ParentView.parentField = ModelView.childField
                                query.where(
                                    new QueryField(mapping.parentField).from(ParentView)
                                ).equal(
                                    new QueryField(mapping.childField).from(ModelView)
                                );
                                const event = {
                                    model: parentModel, // set model, the instance of parent model of the current association
                                    emitter: q, // set event emitter, the instance of data queryable
                                    query: query, // set query, the instance of the modified query expression
                                    target: null
                                }
                                // call method again recursively for parent model to include nested associations and apply permissions
                                void (function(event, executeCallback) { return executeCallback(); })(event, (err) => {
                                   if (err) {
                                       return callback(err);
                                   }
                                    void new DataPermissionEventListener().beforeExecute(event, (err) => {
                                        if (err) {
                                            return cb(err);
                                        }
                                        selectFields.push({
                                            [mapping.childField]: {
                                                $query: query
                                            }
                                        });
                                        // remove field from select clause
                                        const index = selectFields.findIndex((field) => {
                                            return field.$name === `${selectView}.${mapping.childField}`;
                                        });
                                        if (index >= 0) {
                                            selectFields.splice(index, 1);
                                        }
                                        Object.assign(mapping, {
                                            cancel: true
                                        });
                                        return cb();
                                    });
                                });
                            }).catch((err) => {
                                return cb(err);
                            });
                        }).catch((err) => {
                            return cb(err);
                        })
                    } else if (mapping.associationType === 'junction' && mapping.childModel == null && mapping.parentModel === model.name) {
                        /**
                         * @type {import('@themost/data').DataObjectTag}
                         */
                        const property = model.convert({}).property(mapping.refersTo);
                        const baseModel = property.getBaseModel();
                        const attribute = model.getAttribute(mapping.refersTo);
                        if (attribute.type === 'Json' && attribute.additionalType !== 'null') {
                            // upgrade base model
                            void baseModel.migrateAsync().then(() => {
                                const { viewAdapter: BaseView } = baseModel;
                                const additionalModel = context.model(attribute.additionalType);
                                const q = baseModel.asQueryable().select(
                                    ...additionalModel.attributes.map((attribute) => {
                                        return `${mapping.associationValueField}/${attribute.name} as ${attribute.name}`;
                                    })
                                );
                                const { query } = q.prepare();
                                query.where(
                                    new QueryField(mapping.parentField).from(ModelView)
                                ).equal(
                                    new QueryField(mapping.associationObjectField).from(BaseView)
                                );
                                selectFields.push({
                                    [mapping.refersTo]: {
                                        $jsonArray: [
                                            query
                                        ]
                                    }
                                });
                                Object.assign(mapping, {
                                    cancel: true
                                });
                                return cb();
                            });
                        } else {
                            // cannot expand non-json attribute
                            return cb();
                        }
                    } else {
                        return cb();
                    }
                }, (err) => {
                    // remove cancelled mappings
                    for (let i = mappings.length - 1; i >= 0; i--) {
                        // noinspection JSUnresolvedReference
                        if (mappings[i].cancel) {
                            $expand.splice(i, 1);
                        }
                    }
                    if (err) {
                        return callback(err);
                    }
                    return callback();
                });
            }
        } catch (err) {
            return callback(err);
        }

    }
    return callback();
}

export {
    beforeExecute
}