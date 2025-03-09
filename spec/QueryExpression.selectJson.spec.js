// noinspection SpellCheckingInspection

import {MemberExpression, MethodCallExpression, QueryEntity, QueryExpression, QueryField} from '@themost/query';
import { MySqlFormatter } from '../src';
import SimpleOrderSchema from './config/models/SimpleOrder.json';
import {TestApplication} from './TestApplication';


/**
 * @param { import('../src').MySqlAdapter } db
 * @returns {Promise<void>}
 */
async function createSimpleOrders(db) {
    const { source } = SimpleOrderSchema;
    const exists = await db.table(source).existsAsync();
    if (!exists) {
        await db.table(source).createAsync(SimpleOrderSchema.fields);    
    } else {
        return;
    }
    // get some orders
    const orders = await db.executeAsync(
        new QueryExpression().from('OrderBase').select(
            ({orderDate, discount, discountCode, orderNumber, paymentDue,
                 dateCreated, dateModified, createdBy, modifiedBy,
                 orderStatus, orderedItem, paymentMethod, customer}) => {
                return { orderDate, discount, discountCode, orderNumber, paymentDue,
                    dateCreated, dateModified, createdBy, modifiedBy,
                    orderStatus, orderedItem, paymentMethod, customer};
            })
            .orderByDescending((x) => x.orderDate).take(10), []
    );
    const paymentMethods = await db.executeAsync(
        new QueryExpression().from('PaymentMethodBase').select(
            ({id, name, alternateName, description}) => {
                return { id, name, alternateName, description };
            }), []
    );
    const orderStatusTypes = await db.executeAsync(
        new QueryExpression().from('OrderStatusTypeBase').select(
            ({id, name, alternateName, description}) => {
                return { id, name, alternateName, description };
        }), []
    );
    const orderedItems = await db.executeAsync(
        new QueryExpression().from('ProductData').select(
            ({id, name, category, model, releaseDate, price}) => {
                return { id, name, category, model, releaseDate, price };
            }), []
    );
    const customers = await db.executeAsync(
        new QueryExpression().from('PersonData').select(
            ({id, familyName, givenName, jobTitle, email, description, address}) => {
                return { id, familyName, givenName, jobTitle, email, description, address };
            }), []
    );
    const postalAddresses = await db.executeAsync(
        new QueryExpression().from('PostalAddressData').select(
            ({id, streetAddress, postalCode, addressLocality, addressCountry, telephone}) => {
                return {id, streetAddress, postalCode, addressLocality, addressCountry, telephone };
            }), []
    );

    const shuffleArray = (array) => {
        for (let i = array.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [array[i], array[j]] = [array[j], array[i]];
        }
        return array;
    };
    
    const getRandomItems = (array, numItems) => {
        const shuffledArray = shuffleArray([...array]);
        return shuffledArray.slice(0, numItems);
    };
    const items = orders.map((order) => {
        const { orderDate, discount, discountCode, orderNumber, paymentDue,
        dateCreated, dateModified, createdBy, modifiedBy } = order;
        const orderStatus = orderStatusTypes.find((x) => x.id === order.orderStatus);
        const orderedItem = orderedItems.find((x) => x.id === order.orderedItem);
        const paymentMethod = paymentMethods.find((x) => x.id === order.paymentMethod);
        const customer = customers.find((x) => x.id === order.customer);
        if (customer) {
            customer.address = postalAddresses.find((x) => x.id === customer.address);
            delete customer.address?.id;
        }
        // get 2 random payment methods
        const additionalPaymentMethods = getRandomItems(paymentMethods, 2);
        return {
            orderDate,
            discount,
            discountCode,
            orderNumber,
            paymentDue,
            orderStatus,
            orderedItem,
            paymentMethod,
            additionalPaymentMethods,
            customer,
            dateCreated,
            dateModified,
            createdBy,
            modifiedBy
        }
    });
    for (const item of items) {
        await db.executeAsync(new QueryExpression().insert(item).into(source), []);
    }
}

/**
 *
 * @param {{object: any, member: any, target: { $collection: string }, fullyQualifiedMember: string}} event
 */
function onResolvingJsonMember(event) {
    let member = event.fullyQualifiedMember.split('.');
    const field = SimpleOrderSchema.fields.find((x) => x.name === member[0]);
    if (field == null) {
        return;
    }
    if (field.type !== 'Json') {
        return;
    }
    event.object = event.target.$collection;
    // noinspection JSCheckFunctionSignatures
    event.member = new MethodCallExpression('jsonGet', [
        new MemberExpression(event.target.$collection + '.' + event.fullyQualifiedMember)
    ]);
}

describe('SqlFormatter', () => {

    /**
     * @type {TestApplication}
     */
    let app;
    let context;
    beforeAll(async () => {
        app = new TestApplication(__dirname);
        context = app.createContext();
        const {db} = context;
        await createSimpleOrders(db);
    });
    afterAll(async () => {
        await app.finalize();
    });
    beforeEach(async () => {
        await context.finalizeAsync();
    });

    it('should select json field', async () => {
        await app.executeInTestTranscaction(async (context) => {
            const Orders = new QueryEntity('SimpleOrders');
            const query = new QueryExpression();
            query.resolvingJoinMember.subscribe(onResolvingJsonMember);
            query.select((x) => {
                // noinspection JSUnresolvedReference
                return {
                    id: x.id,
                    customer: x.customer.description
                }
            })
                .from(Orders);
            const formatter = new MySqlFormatter();
            const sql = formatter.format(query);
            expect(sql).toEqual('SELECT `SimpleOrders`.`id` AS `id`, json_extract(`SimpleOrders`.`customer`, \'$.description\') AS `customer` FROM `SimpleOrders`');
            /**
             * @type {Array<{id: number, customer: string}>}
             */
            const results = await context.db.executeAsync(sql, []);
            expect(results).toBeTruthy();
            for (const result of results) {
                expect(result).toBeTruthy();
                expect(result.id).toBeTruthy();
                expect(result.customer).toBeTruthy();
            }
        });
    });

    it('should select nested json field', async () => {
        await app.executeInTestTranscaction(async (context) => {
            const Orders = new QueryEntity('SimpleOrders');
            const query = new QueryExpression();
            query.resolvingJoinMember.subscribe(onResolvingJsonMember);
            query.select((x) => {
                // noinspection JSUnresolvedReference
                return {
                    id: x.id,
                    customer: x.customer.description,
                    address: x.customer.address.streetAddress
                }
            })
                .from(Orders);
            const formatter = new MySqlFormatter();
            const sql = formatter.format(query);
            expect(sql).toEqual('SELECT `SimpleOrders`.`id` AS `id`, ' +
                'json_extract(`SimpleOrders`.`customer`, \'$.description\') AS `customer`, ' +
                'json_extract(`SimpleOrders`.`customer`, \'$.address.streetAddress\') AS `address` ' +
                'FROM `SimpleOrders`');
            /**
             * @type {Array<{id: number, customer: string}>}
             */
            const results = await context.db.executeAsync(sql, []);
            expect(results).toBeTruthy();
            for (const result of results) {
                expect(result).toBeTruthy();
                expect(result.id).toBeTruthy();
                expect(result.customer).toBeTruthy();
            }
        });
    });

    it('should select nested json field with method', async () => {
        await app.executeInTestTranscaction(async (context) => {
            const Orders = new QueryEntity('SimpleOrders');
            const query = new QueryExpression();
            query.resolvingJoinMember.subscribe(onResolvingJsonMember);
            query.select((x) => {
                // noinspection JSUnresolvedReference
                return {
                    id: x.id,
                    customer: x.customer.description,
                    releaseYear: x.orderedItem.releaseDate.getFullYear()
                }
            })
                .from(Orders);
            const formatter = new MySqlFormatter();
            const sql = formatter.format(query);
            /**
             * @type {Array<{id: number, customer: string, releaseYear: number}>}
             */
            const results = await context.db.executeAsync(sql, []);
            expect(results).toBeTruthy();
            for (const result of results) {
                expect(result).toBeTruthy();
                expect(result.releaseYear).toBeTruthy();
            }
        });
    });

    it('should select json object', async () => {
        await app.executeInTestTranscaction(async (context) => {
            const Orders = new QueryEntity('SimpleOrders');
            const query = new QueryExpression();
            query.resolvingJoinMember.subscribe(onResolvingJsonMember);
            query.select((x) => {
                // noinspection JSUnresolvedReference
                return {
                    id: x.id,
                    customer: x.customer,
                    orderedItem: x.orderedItem
                }
            })
                .from(Orders);
            const formatter = new MySqlFormatter();
            const sql = formatter.format(query);
            /**
             * @type {Array<{id: number, customer: string, releaseYear: number}>}
             */
            const results = await context.db.executeAsync(sql, []);
            expect(results).toBeTruthy();
            for (const result of results) {
                const {customer} =result;
                expect(customer).toBeTruthy();
            }
        });
    });

    it('should select and return attribute from json field using closures', async () => {
        await app.executeInTestTranscaction(async (context) => {
            const Orders = context.model('SimpleOrder').silent();
            const results = await Orders.select((x) => {
                return {
                    id: x.id,
                    customer: x.customer.description,
                    streetAddress: x.customer.address.streetAddress,
                    releaseYear: x.orderedItem.releaseDate.getFullYear()
                }
            }).getItems();
            expect(results).toBeTruthy();
            for (const result of results) {
                expect(result).toBeTruthy();
                expect(result.releaseYear).toBeTruthy();
            }
        });
    });

    it('should filter results using attribute extracted from json field', async () => {
        await app.executeInTestTranscaction(async (context) => {
            const Orders = context.model('SimpleOrder').silent();
            const results = await Orders.select((x) => {
                return {
                    id: x.id,
                    customerIdentifier: x.customer.id,
                    customer: x.customer.description
                }
            })
                .where((x) => x.customer.description === 'Eric Thomas')
                .getItems();
            expect(results).toBeTruthy();
            for (const result of results) {
                expect(result).toBeTruthy();
                expect(result.customer).toEqual('Eric Thomas');
            }
        });
    });

    it('should select and return attribute from json field', async () => {
        await app.executeInTestTranscaction(async (context) => {
            const Orders = context.model('SimpleOrder').silent();
            const q = await Orders.filterAsync({
                $select: 'id,customer/description as customer,year(orderedItem/releaseDate) as releaseYear',
            })
            const results = await q.getItems();
            expect(results).toBeTruthy();
            for (const result of results) {
                expect(result).toBeTruthy();
                expect(result.releaseYear).toBeTruthy();
            }
        });
    });

    it('should filter using attribute from json field', async () => {
        await app.executeInTestTranscaction(async (context) => {
            const Orders = context.model('SimpleOrder').silent();
            const q = await Orders.filterAsync({
                $select: 'id,customer/id as customerIdentifier, customer/description as customer,year(orderedItem/releaseDate) as releaseYear',
                $filter: 'customer/description eq \'Eric Thomas\''
            })
            const results = await q.getItems();
            expect(results).toBeTruthy();
            for (const result of results) {
                expect(result).toBeTruthy();
                expect(result.customer).toEqual('Eric Thomas');
            }
        });
    });

    it('should use jsonObject', async () => {
        await app.executeInTestTranscaction(async (context) => {
            const Orders = context.model('Order').silent();
            const q = Orders.select(
                'id', 'orderedItem', 'orderDate'
            ).where('customer/description').equal('Eric Thomas');
            const select = q.query.$select[Orders.viewAdapter];
            select.push({
                customer: {
                    $jsonObject: [
                        new QueryField('familyName').from('customer'), // field without alias
                        new QueryField('givenName').from('customer').as('givenName'), // field with alias
                        new QueryField({
                            active: {
                                $value: true
                            }
                        }) // field with value and alias
                    ]
                }
            });
            const items = await q.getItems();
            expect(items).toBeTruthy();
            for (const item of items) {
                expect(item.customer).toBeTruthy();
                expect(item.customer.familyName).toEqual('Thomas');
                expect(item.customer.givenName).toEqual('Eric');
            }
        });
    });

    it('should use jsonObject in ad-hoc queries', async () => {
        await app.executeInTestTranscaction(async (context) => {
            const {viewAdapter: Orders} = context.model('Order');
            const {viewAdapter: Customers} = context.model('Person');
            const {viewAdapter: OrderStatusTypes} = context.model('OrderStatusType');
            const q = new QueryExpression().select(
                'id', 'orderedItem', 'orderStatus', 'orderDate'
            ).from(Orders).join(new QueryEntity(Customers).as('customers')).with(
                new QueryExpression().where(
                    new QueryField('customer').from(Orders)
                ).equal(
                    new QueryField('id').from('customers')
                )
            ).join(new QueryEntity(OrderStatusTypes).as('orderStatusTypes')).with(
                new QueryExpression().where(
                    new QueryField('orderStatus').from(Orders)
                ).equal(
                    new QueryField('id').from('orderStatusTypes')
                )
            ).where(new QueryField('description').from('customers')).equal('Eric Thomas');
            const select = q.$select[Orders];
            select.push({
                customer: {
                    $jsonObject: [
                        new QueryField('familyName').from('customers'),
                        new QueryField('givenName').from('customers'),
                    ]
                }
            }, {
                orderStatus: {
                    $jsonObject: [
                        new QueryField('name').from('orderStatusTypes').as('name'),
                        new QueryField('alternateName').from('orderStatusTypes').as('alternateName'),
                    ]
                }
            });
            /**
             * @type {Array<{id: number, orderedItem: number, orderDate: Date, orderStatus: { name: string, alternateName: string }, customer: {familyName: string, givenName: string}}>}
             */
            const items = await context.db.executeAsync(q, []);
            expect(items).toBeTruthy();
            for (const item of items) {
                expect(item.customer).toBeTruthy();
                expect(item.customer.familyName).toEqual('Thomas');
                expect(item.customer.givenName).toEqual('Eric');
                expect(item.orderStatus).toBeTruthy();
                expect(item.orderStatus.name).toBeTruthy();
            }

        });
    });

});
