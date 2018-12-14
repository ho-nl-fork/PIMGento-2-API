<?php

namespace Pimgento\Api\Job;

use Akeneo\Pim\ApiClient\Pagination\PageInterface;
use Akeneo\Pim\ApiClient\Pagination\ResourceCursorInterface;
use Magento\Catalog\Model\Product\Link;
use Magento\Catalog\Model\ProductLink\Link as ProductLink;
use Magento\Catalog\Model\Product\Visibility;
use Magento\Catalog\Model\Product as ProductModel;
use Magento\Catalog\Model\Category as CategoryModel;
use Magento\Framework\App\Cache\Type\Block;
use Magento\Framework\App\Config\ScopeConfigInterface;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\DB\Select;
use Magento\Framework\Event\ManagerInterface;
use Magento\Framework\Exception\LocalizedException;
use Magento\Framework\App\Cache\TypeListInterface;
use Magento\PageCache\Model\Cache\Type;
use Magento\CatalogUrlRewrite\Model\ProductUrlPathGenerator;
use Magento\CatalogUrlRewrite\Model\ProductUrlRewriteGenerator;
use Magento\Staging\Model\VersionManager;
use Magento\Catalog\Model\Product\Attribute\Backend\Media\ImageEntryConverter;
use Pimgento\Api\Helper\Authenticator;
use Pimgento\Api\Helper\Config as ConfigHelper;
use Pimgento\Api\Helper\Output as OutputHelper;
use Pimgento\Api\Helper\Store as StoreHelper;
use Pimgento\Api\Helper\ProductFilters;
use Pimgento\Api\Helper\Serializer as JsonSerializer;
use Pimgento\Api\Helper\Import\Product as ProductImportHelper;
use Psr\Log\LoggerInterface;
use Zend_Db_Expr as Expr;
use Zend_Db_Statement_Pdo;

/**
 * Class Product
 *
 * @category  Class
 * @package   Pimgento\Api\Job
 * @author    Agence Dn'D <contact@dnd.fr>
 * @copyright 2018 Agence Dn'D
 * @license   http://opensource.org/licenses/osl-3.0.php  Open Software License (OSL 3.0)
 * @link      https://www.pimgento.com/
 */
class Product extends Import
{
    /**
     * @var string PIM_PRODUCT_STATUS_DISABLED
     */
    const PIM_PRODUCT_STATUS_DISABLED = '0';
    /**
     * @var string MAGENTO_PRODUCT_STATUS_DISABLED
     */
    const MAGENTO_PRODUCT_STATUS_DISABLED = '2';
    /**
     * @var int CONFIGURABLE_INSERTION_MAX_SIZE
     */
    const CONFIGURABLE_INSERTION_MAX_SIZE = 500;

    /**
     * This variable contains a string value
     *
     * @var string $code
     */
    protected $code = 'product';
    /**
     * This variable contains a string value
     *
     * @var string $name
     */
    protected $name = 'Product';
    /**
     * list of allowed type_id that can be imported
     *
     * @var string[]
     */
    protected $allowedTypeId = ['simple', 'virtual'];
    /**
     * This variable contains a ProductImportHelper
     *
     * @var ProductImportHelper $entitiesHelper
     */
    protected $entitiesHelper;
    /**
     * This variable contains a ConfigHelper
     *
     * @var ConfigHelper $configHelper
     */
    protected $configHelper;
    /**
     * This variable contains a ProductFilters
     *
     * @var ProductFilters $productFilters
     */
    protected $productFilters;
    /**
     * This variable contains a ScopeConfigInterface
     *
     * @var ScopeConfigInterface $scopeConfig
     */
    protected $scopeConfig;
    /**
     * This variable contains a JsonSerializer
     *
     * @var JsonSerializer $serializer
     */
    protected $serializer;
    /**
     * This variable contains a ProductModel
     *
     * @var ProductModel $product
     */
    protected $product;
    /**
     * This variable containsa ProductUrlPathGenerator
     *
     * @var ProductUrlPathGenerator $productUrlPathGenerator
     */
    protected $productUrlPathGenerator;
    /**
     * This variable contains a TypeListInterface
     *
     * @var TypeListInterface $cacheTypeList
     */
    protected $cacheTypeList;
    /**
     * This variable contains a StoreHelper
     *
     * @var StoreHelper $storeHelper
     */
    protected $storeHelper;
    /**
     * @var string $configurableTmpTableSuffix
     */
    protected $configurableTmpTableSuffix;
    /**
     * @var Option $optionJob
     */
    protected $optionJob;

    /**
     * Product constructor.
     *
     * @param OutputHelper $outputHelper
     * @param ManagerInterface $eventManager
     * @param Authenticator $authenticator
     * @param ProductImportHelper $entitiesHelper
     * @param ConfigHelper $configHelper
     * @param ProductFilters $productFilters
     * @param ScopeConfigInterface $scopeConfig
     * @param JsonSerializer $serializer
     * @param ProductModel $product
     * @param ProductUrlPathGenerator $productUrlPathGenerator
     * @param TypeListInterface $cacheTypeList
     * @param StoreHelper $storeHelper
     * @param Option $optionJob
     * @param LoggerInterface $logger
     * @param array $data
     */
    public function __construct(
        OutputHelper $outputHelper,
        ManagerInterface $eventManager,
        Authenticator $authenticator,
        \Psr\Log\LoggerInterface $logger,
        ProductImportHelper $entitiesHelper,
        ConfigHelper $configHelper,
        ProductFilters $productFilters,
        ScopeConfigInterface $scopeConfig,
        JsonSerializer $serializer,
        ProductModel $product,
        ProductUrlPathGenerator $productUrlPathGenerator,
        TypeListInterface $cacheTypeList,
        StoreHelper $storeHelper,
        Option $optionJob,
        array $data = []
    ) {
        parent::__construct($outputHelper, $eventManager, $authenticator, $logger, $data);

        $this->entitiesHelper          = $entitiesHelper;
        $this->configHelper            = $configHelper;
        $this->productFilters          = $productFilters;
        $this->scopeConfig             = $scopeConfig;
        $this->serializer              = $serializer;
        $this->product                 = $product;
        $this->cacheTypeList           = $cacheTypeList;
        $this->storeHelper             = $storeHelper;
        $this->productUrlPathGenerator = $productUrlPathGenerator;
        $this->optionJob               = $optionJob;
        $this->configurableTmpTableSuffix = 'configurable';
    }

    /**
     * Create temporary table:
     * Grab a product from Akeneo API, examine its structure and determine the appropriate
     * table structure.
     *
     * @return void
     */
    public function createTable()
    {
        /** @var PageInterface $products */
        $products = $this->akeneoClient->getProductApi()->listPerPage(1);
        /** @var array $products */
        $products = $products->getItems();
        if (empty($products)) {
            $this->setMessage(__('No results from Akeneo'));
            $this->stop(true);

            return;
        }

        /** @var array $product */
        $product = reset($products); // Grabs the first item of array.

        // Create the table used for additional configurable processing.
        // Table name: tmp_pimgento_entities_configurable
        $this->entitiesHelper->createTmpTableFromApi($product, $this->getCode());

        // Create the table used for final product import.
        // Table name: tmp_pimgento_entities_product.
        $this->entitiesHelper->createTmpTable([], $this->configurableTmpTableSuffix);
        $productTmpTable = $this->entitiesHelper->getTableName($this->getCode());
        $configurableTmpTable = $this->entitiesHelper->getTableName($this->configurableTmpTableSuffix);
        $connection = $this->entitiesHelper->getConnection();
        $connection->addColumn($configurableTmpTable, 'identifier', [
            // This column holds info as to whether a product is "simple" or "configurable"
            'type' => 'text',
            'length' => 255,
            'default' => '',
            'COMMENT' => ' ',
            'nullable' => false
        ]);
        $additionalColNames = ['family', 'categories'];
        foreach ($additionalColNames as $colName) {
            if ($connection->tableColumnExists($productTmpTable, $colName)) {
                $connection->addColumn($configurableTmpTable, $colName, 'text');
            }
        }
        // Note that the tmp_pimgento_entities_configurable table will also have an entity_id column,
        // created automatically by the EntitiesHelper and whose value will remain null.
    }

    /**
     * Insert data into "final" temporary table.
     *
     * @return void
     */
    public function insertData()
    {
        /** @var array $filters */
        $filters = $this->productFilters->getFilters();
        /** @var string|int $paginationSize */
        $paginationSize = $this->configHelper->getPanigationSize();
        /** @var ResourceCursorInterface $productModels */
        $products = $this->akeneoClient->getProductApi()->all($paginationSize, $filters);
        /** @var int $index */
        $index = 0;
        /**
         * @var int $index
         * @var array $product
         */
        foreach ($products as $index => $product) {
            $this->entitiesHelper->insertDataFromApi($product, $this->getCode());
        }
        if ($index) {
            $index++;
        }

        $this->setMessage(
            __('%1 line(s) found', $index)
        );
    }

    /**
     * Enrich temporary tables before processing
     *
     * @return void
     * @throws LocalizedException
     */
    public function addRequiredData()
    {
        $productTmpTable = $this->entitiesHelper->getTableName($this->getCode());
        $configurableTmpTable = $this->entitiesHelper->getTableName($this->configurableTmpTableSuffix);
        $tmpTables = [$productTmpTable, $configurableTmpTable];
        $connection = $this->entitiesHelper->getConnection();

        // What attributes have been forked during the FamilyVariant import job?
        // $attributeForks contains K=>V pairs
        // like FORKED_CODE => ['code_fork'=>FORKED_CODE, 'code_orig'=> ORIGINAL_CODE, 'unit'=>UNIT]

        /** @var Select $select */
        $select = $connection->select()->from(
            FamilyVariant::FORKED_ATTRIBUTE_TABLE_NAME,
            [
                'code_fork' => 'code' . FamilyVariant::FAMILY_FORK_SUFFIX,
                'code_orig' => 'code',
                'unit' => 'code' . FamilyVariant::METRIC_UNIT_SUFFIX
            ]
        )   ->where('code' . FamilyVariant::FAMILY_FORK_SUFFIX . ' IS NOT NULL')
            ->where('code' . FamilyVariant::FAMILY_FORK_SUFFIX . ' <> ""');
        /** @var array $data */
        $attributeForks = $connection->fetchAssoc($select);

        if (count($attributeForks)) {

            // Keep only those forked attributes that are present in the Product tmp table.

            foreach ($attributeForks as $forkedCode => $attributeFork) {
                if (!$connection->tableColumnExists($productTmpTable, $attributeFork['code_orig'])) {
                    unset($attributeForks[$forkedCode]);
                }
            }

            if (count($attributeForks)) { // $attributeForks might have been emptied.

                // Prepare to write the new options to an Options tmp table.
                $this->optionJob->createTable();

                // Get the locales for which the options will need labels.
                /** @var array $optionsTableColumns */
                $optionsTableColumns = array_keys(
                    $connection->describeTable(
                        $this->entitiesHelper->getTableName($this->optionJob->getCode())
                    )
                );
                /** @var array $localeSuffixes */
                $localeSuffixes = [];
                foreach ($optionsTableColumns as $title) {
                    $parts = explode('-', $title);
                    if ($parts[0] === 'labels') {
                        $localeSuffixes[] = $parts[1];
                    }
                }

                foreach ($attributeForks as $forkedCode => $attributeFork) {

                    // On the Product tmp table front, rename columns that reference a forked attribute.

                    $sql = 'ALTER TABLE '
                        . $productTmpTable
                        . ' CHANGE ' . $attributeFork['code_orig'] . ' ' . $forkedCode . ' text';
                    $connection->query($sql);

                    // On the Options front, make a list of all options present in the table.

                    $select = $connection->select()
                        ->from($productTmpTable, $forkedCode)
                        ->where($forkedCode . '!=""')
                        ->where($forkedCode . ' IS NOT NULL');
                    /** @var \Magento\Framework\DB\Statement\Pdo\Mysql $query */
                    $query = $connection->query($select);

                    $options = [];
                    while ($row = $query->fetch()) {
                        $options[] = $row[$forkedCode];
                    }
                    $options = array_unique($options);

                    foreach ($options as $option) {
                        $data = [
                            'code'          => $option,
                            'attribute'     => $forkedCode,
                        ];
                        // Add labels for each locale.
                        foreach ($localeSuffixes as $localeSuffix) {
                            $data['labels-' . $localeSuffix] = $option . ' ' . $attributeFork['unit'];
                        }
                        // Write data to the Options tmp table.
                        $connection->insertOnDuplicate(
                            $this->entitiesHelper->getTableName($this->optionJob->getCode()),
                            $data
                        );
                    }

                }

                // Complete the Options import job.
                $this->optionJob->matchEntities();
                $this->optionJob->insertOptions();
                $this->optionJob->insertValues();
                $this->optionJob->dropTable();
                $this->optionJob->cleanCache();
            }
        }

        // Resume usual course of operations.

        foreach ($tmpTables as $tmpTable) {
            $connection->addColumn($tmpTable, '_type_id', [
                // This column holds info as to whether a product is "simple" or "configurable"
                'type' => 'text',
                'length' => 255,
                'default' => 'simple',
                'COMMENT' => ' ',
                'nullable' => false
            ]);
            $connection->addColumn($tmpTable, '_options_container', [
                'type' => 'text',
                'length' => 255,
                'default' => 'container2',
                'COMMENT' => ' ',
                'nullable' => false
            ]);
            $connection->addColumn($tmpTable, '_tax_class_id', [
                'type' => 'integer',
                'length' => 11,
                'default' => 0,
                'COMMENT' => ' ',
                'nullable' => false
            ]);// None
            $connection->addColumn($tmpTable, '_attribute_set_id', [
                'type' => 'integer',
                'length' => 11,
                'default' => 4,
                'COMMENT' => ' ',
                'nullable' => false
            ]);// Default
            $connection->addColumn($tmpTable, '_visibility', [
                'type' => 'integer',
                'length' => 11,
                'default' => Visibility::VISIBILITY_BOTH,
                'COMMENT' => ' ',
                'nullable' => false
            ]);
            $connection->addColumn($tmpTable, '_status', [
                'type' => 'integer',
                'length' => 11,
                'default' => 2,
                'COMMENT' => ' ',
                'nullable' => false
            ]);
            if (!$connection->tableColumnExists($tmpTable, 'url_key')) {
                $connection->addColumn($tmpTable, 'url_key', [
                    'type' => 'text',
                    'length' => 255,
                    'default' => '',
                    'COMMENT' => ' ',
                    'nullable' => false
                ]);
                $connection->update($tmpTable, ['url_key' => new Expr('LOWER(`identifier`)')]);
            }
            if ($connection->tableColumnExists($tmpTable, 'enabled')) {
                $connection->update($tmpTable, ['_status' => new Expr('IF(`enabled` <> 1, 2, 1)')]);
            }
            /** @var string|null $groupColumn */
            $groupColumn = null;
            // The 2 columns upon which is based the decision to make the product "simple" or "configurable"
            // are "parent" and "group". "parent" has precedence over "group".
            if ($connection->tableColumnExists($tmpTable, 'parent')) {
                $groupColumn = 'parent';
            }
            if ($connection->tableColumnExists($tmpTable, 'groups') && !$groupColumn) {
                $groupColumn = 'groups';
            }
            if ($groupColumn) {
                $connection->update(
                    $tmpTable,
                    [
                        '_visibility' => new Expr(
                        // If there is a product hierarchy, either in the form of a "parent" or of a "group" column, make the product visible.
                        // Otherwise, hide it.
                            'IF(`' . $groupColumn . '` <> "", ' . Visibility::VISIBILITY_NOT_VISIBLE . ', ' . Visibility::VISIBILITY_BOTH . ')'
                        ),
                    ]
                );
            }
            if ($connection->tableColumnExists($tmpTable, 'type_id')) {
                /** @var string $types */
                $types = $connection->quote($this->allowedTypeId); // Possible values: "simple", "virtual".
                $connection->update(
                    $tmpTable,
                    [
                        // Set "simple" product type.
                        '_type_id' => new Expr("IF(`type_id` IN ($types), `type_id`, 'simple')"),
                    ]
                );
            }

            // Map PIM attributes to Magento attributes.

            /** @var string|array $matches */
            $matches = $this->scopeConfig->getValue(ConfigHelper::PRODUCT_ATTRIBUTE_MAPPING);
            $matches = $this->serializer->unserialize($matches);
            // $matches is an array like [['pim_attribute' => value, 'magento_attribute' => value], ..]
            if (!is_array($matches)) {
                return;
            }
            /** @var array $stores */
            $stores = $this->storeHelper->getAllStores();
            /** @var array $match */
            foreach ($matches as $match) {
                if (!isset($match['pim_attribute'], $match['magento_attribute'])) {
                    continue;
                }

                /** @var string $pimAttribute */
                $pimAttribute = $match['pim_attribute'];
                /** @var string $magentoAttribute */
                $magentoAttribute = $match['magento_attribute'];

                $this->entitiesHelper->copyColumn($tmpTable, $pimAttribute, $magentoAttribute);

                /**
                 * @var string $local
                 * @var string $affected
                 */
                foreach ($stores as $local => $affected) {
                    $this->entitiesHelper->copyColumn(
                        $tmpTable,
                        $pimAttribute . '-' . $local,
                        $magentoAttribute . '-' . $local
                    );
                }
            }

        }
    }

    /**
     * Create configurables
     *
     * @return void
     * @throws LocalizedException
     */
    public function createConfigurable()
    {
        // TODO Refactor the two rounds of configurable processing into 2 separate methods,
        // called by different steps of the Product-import process.

        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();

        // The first round of configurable determination writes the immediate parents of simple products
        // to the intermediate tmp table, $configurableTmpTable (tmp_pimgento_entities_configurable).

        /** @var string $productTmpTable */
        $productTmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var string $configurableTmpTable */
        $configurableTmpTable = $this->entitiesHelper->getTableName($this->configurableTmpTableSuffix);
        /** @var array $tmpTables */
        $tmpTables = [$productTmpTable, $configurableTmpTable];

        // Determine which column is to be used to find out a simple product's parent.

        /** @var string|null $groupColumn */
        $groupColumn = null;
        if ($connection->tableColumnExists($productTmpTable, 'parent')) {
            $groupColumn = 'parent';
        }
        if (!$groupColumn && $connection->tableColumnExists($productTmpTable, 'groups')) {
            $groupColumn = 'groups';
        }
        if (!$groupColumn) {
            $this->setStatus(false);
            $this->setMessage(__('Columns groups or parent not found'));

            return;
        }

        foreach ($tmpTables as $tmpTable) {
            // The _children column will hold a list referencing all children of the configurable.
            $connection->addColumn($tmpTable, '_children', 'text');
        }
        $connection->addColumn($productTmpTable, '_axis', [
            'type' => 'text',
            'length' => 255,
            'default' => '',
            'COMMENT' => ' '
        ]);

        /** @var array $data */
        $data = [
            'identifier'         => 'e.' . $groupColumn,
            '_children'          => new Expr('GROUP_CONCAT(e.identifier SEPARATOR ",")')
        ]; // $data keys = aliases in SQL query.

        if ($connection->tableColumnExists($productTmpTable, 'family')) {
            $data['family'] = 'e.family';
        }
        if ($connection->tableColumnExists($productTmpTable, 'categories')) {
            $data['categories'] = 'e.categories';
        }

        /** @var string|array $additional */
        $additional = $this->scopeConfig->getValue(ConfigHelper::PRODUCT_CONFIGURABLE_ATTRIBUTES);
        $additional = $this->serializer->unserialize($additional);
        if (!is_array($additional)) {
            $additional = [];
        }

        /** @var array $stores */
        $stores = $this->storeHelper->getAllStores();
        /** @var string $variantTable */
        $variantTable = $connection->getTableName('pimgento_product_model');

        /** @var array $attribute */
        foreach ($additional as $attribute) {
            if (!isset($attribute['attribute'], $attribute['value'])) {
                continue;
            }

            /** @var string $name */
            $name = $attribute['attribute'];
            /** @var string $value */
            $value = $attribute['value'];
            /** @var array $columns */
            $columns = [trim($name)];

            /**
             * @var string $local
             * @var string $affected
             */
            foreach ($stores as $local => $affected) {
                $columns[] = trim($name) . '-' . $local;
            }

            /** @var array $column */
            foreach ($columns as $column) {
                if ($column === 'enabled' && $connection->tableColumnExists($productTmpTable, 'enabled')) {
                    $column = '_status';
                    if ($value === self::PIM_PRODUCT_STATUS_DISABLED) {
                        $value = self::MAGENTO_PRODUCT_STATUS_DISABLED;
                    }
                }

                if (!$connection->tableColumnExists($productTmpTable, $column)) {
                    continue;
                }

                if (strlen($value) > 0) {
                    $data[$column] = new Expr('"' . $value . '"');

                    continue;
                }

                $data[$column] = 'e.' . $column;
                if ($connection->tableColumnExists($variantTable, $column)) {
                    $data[$column] = 'v.' . $column;
                }
            }
        }

        /** @var Select $configurable */
        $configurable = $connection->select()
            ->from(['e' => $productTmpTable], $data)
            ->joinInner(['v' => $variantTable],'e.' . $groupColumn . ' = v.code', [])
            ->where('e.' . $groupColumn.' <> ""')
            ->group('e.' . $groupColumn);

        /** @var string $query */
        $query = $connection->insertFromSelect($configurable, $configurableTmpTable, array_keys($data));

        $connection->query($query);

        // This second round of processing determines if the configurables now listed in
        // the intermediate temporary table (tmp_pimgento_entities_configurable) themselves have parents.

        // For configurables that have a parent, get that parent and insert it into
        // the "final" temporary table, $productTmpTable (tmp_pimgento_entities_product).

        /** @var array $data */
        $data = [
            'identifier'         => 'v.parent',
            'url_key'            => 'v.parent',
            '_children'          => new Expr('GROUP_CONCAT(e._children SEPARATOR ",")'),
            '_type_id'           => new Expr('"configurable"'),
            '_options_container' => new Expr('"container1"'),
            '_status'            => 'e._status',
            '_axis'              => 'v.axis',
        ];

        if ($connection->tableColumnExists($configurableTmpTable, 'family')) {
            $data['family'] = 'e.family';
        }
        if ($connection->tableColumnExists($configurableTmpTable, 'categories')) {
            $data['categories'] = 'e.categories';
        }

        // TODO determine whether loop for $additional might be necessary / functional here.

        /** @var Select $configurable */
        $configurable = $connection->select()
            ->from(['e' => $configurableTmpTable], $data)
            ->joinInner(['v' => $variantTable],'e.identifier = v.code', [])
            ->where('v.parent <> ""')
            ->group('v.parent');

        /** @var string $query */
        $query = $connection->insertFromSelect($configurable, $productTmpTable, array_keys($data));

        $connection->query($query);

        // For configurables that don't have a parent, retain that configurable _as is_
        // and insert it into the "final" temporary table, $productTmpTable (tmp_pimgento_entities_product).

        $data = [
            'identifier'         => 'e.identifier',
            'url_key'            => 'e.url_key',
            '_children'          => 'e._children',
            '_type_id'           => new Expr('"configurable"'),
            '_options_container' => new Expr('"container1"'),
            '_status'            => 'e._status',
            '_axis'              => 'v.axis',
        ];

        if ($connection->tableColumnExists($configurableTmpTable, 'family')) {
            $data['family'] = 'e.family';
        }
        if ($connection->tableColumnExists($configurableTmpTable, 'categories')) {
            $data['categories'] = 'e.categories';
        }

        // TODO determine whether loop for $additional might be necessary / functional here.

        $configurable = $connection->select()
            ->from(['e' => $configurableTmpTable], $data)
            ->joinInner(['v' => $variantTable],'e.identifier = v.code', [])
            ->where('v.parent = ""');
        // Write actual SQL query to system.log
        $this->logger->info($configurable->assemble());

        $query = $connection->insertFromSelect($configurable, $productTmpTable, array_keys($data));

        $connection->query($query);
    }

    /**
     * Match code with entity
     *
     * @return void
     */
    public function matchEntities()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());

        /** @var array $duplicates */
        $duplicates = $connection->fetchCol(
            $connection->select()
                ->from($tmpTable, ['identifier'])
                ->group('identifier')
                ->having('COUNT(identifier) > ?', 1)
        );

        if (!empty($duplicates)) {
            $this->setMessage(
                __('Duplicates sku detected. Make sure Product Model code is not used for a simple product sku. Duplicates: %1', join(', ', $duplicates))
            );
            $this->stop(true);

            return;
        }

        $this->entitiesHelper->matchEntity(
            'identifier',
            'catalog_product_entity',
            'entity_id',
            $this->getCode()
        );
    }

    /**
     * Update product attribute set id
     *
     * @return void
     */
    public function updateAttributeSetId()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());

        if (!$connection->tableColumnExists($tmpTable,'family')) {
            $this->setStatus(false);
            $this->setMessage(__('Column family is missing'));

            return;
        }

        /** @var string $entitiesTable */
        $entitiesTable = $connection->getTableName('pimgento_entities');
        /** @var Select $families */
        $families = $connection->select()
            ->from(false, ['_attribute_set_id' => 'c.entity_id'])
            ->joinLeft(['c' => $entitiesTable],'p.family = c.code AND c.import = "family"', []);

        $connection->query($connection->updateFromSelect($families, ['p' => $tmpTable]));

        /** @var bool $noFamily */
        $noFamily = (bool)$connection->fetchOne(
            $connection->select()->from($tmpTable, ['COUNT(*)'])->where('_attribute_set_id = ?', 0)
        );
        if ($noFamily) {
            $this->setStatus(false);
            $this->setMessage(__('Warning: %1 product(s) without family. Please try to import families.', $noFamily));
        }

        $connection->update(
            $tmpTable,
            ['_attribute_set_id' => $this->product->getDefaultAttributeSetId()],
            ['_attribute_set_id = ?' => 0]
        );
    }

    /**
     * Replace option code by id
     *
     * @return void
     * @throws \Zend_Db_Statement_Exception
     */
    public function updateOption()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var string[] $columns */
        $columns = array_keys($connection->describeTable($tmpTable));
        /** @var string[] $except */
        $except = [
            '_entity_id',
            '_is_new',
            '_status',
            '_type_id',
            '_options_container',
            '_tax_class_id',
            '_attribute_set_id',
            '_visibility',
            '_children',
            '_axis',
            'identifier',
            'categories',
            'family',
            'groups',
            'parent',
            'url_key',
            'enabled',
        ];

        /** @var string $column */
        foreach ($columns as $column) {
            if (in_array($column, $except) || preg_match('/-unit/', $column)) {
                continue;
            }

            if (!$connection->tableColumnExists($tmpTable, $column)) {
                continue;
            }

            /** @var array|string $columnPrefix */
            $columnPrefix = explode('-', $column);
            $columnPrefix = reset($columnPrefix);
            /** @var int $prefixLength */
            $prefixLength = strlen($columnPrefix . '_') + 1;
            /** @var string $entitiesTable */
            $entitiesTable = $connection->getTableName('pimgento_entities');

            // Sub select to increase performance versus FIND_IN_SET
            /** @var Select $subSelect */
            $subSelect = $connection->select()
                ->from(
                    ['c' => $entitiesTable],
                    ['code' => 'SUBSTRING(`c`.`code`, ' . $prefixLength . ')', 'entity_id' => 'c.entity_id']
                )
                ->where('c.code LIKE "' . $columnPrefix . '_%" ')
                ->where('c.import = ?', 'option');

            // if no option no need to continue process
            if (!$connection->query($subSelect)->rowCount()) {
                continue;
            }

            //in case of multiselect
            /** @var string $conditionJoin */
            $conditionJoin = "IF ( locate(',', `".$column."`) > 0 , " . "`p`.`" . $column . "` like " . new Expr(
                    "CONCAT('%', `c1`.`code`, '%')"
                ) . ", `p`.`" . $column . "` = `c1`.`code` )";

            /** @var Select $select */
            $select = $connection->select()
                ->from(
                    ['p' => $tmpTable],
                    ['identifier' => 'p.identifier', 'entity_id' => 'p._entity_id']
                )->joinInner(
                    ['c1' => new Expr('(' . (string)$subSelect . ')')],
                    new Expr($conditionJoin),
                    [$column => new Expr('GROUP_CONCAT(`c1`.`entity_id` SEPARATOR ",")')]
                )->group('p.identifier');

            /** @var string $query */
            $query = $connection->insertFromSelect(
                $select,
                $tmpTable,
                ['identifier', '_entity_id', $column],
                AdapterInterface::INSERT_ON_DUPLICATE
            );

            $connection->query($query);
        }
    }

    /**
     * Create product entities
     *
     * @return void
     */
    public function createEntities()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());

        if ($connection->isTableExists($connection->getTableName('sequence_product'))) {
            /** @var array $values */
            $values  = ['sequence_value' => '_entity_id'];
            /** @var Select $parents */
            $parents = $connection->select()->from($tmpTable, $values);
            /** @var string $query */
            $query = $connection->insertFromSelect(
                $parents,
                $connection->getTableName('sequence_product'),
                array_keys($values),
                AdapterInterface::INSERT_ON_DUPLICATE
            );

            $connection->query($query);
        }

        /** @var string $table */
        $table = $connection->getTableName('catalog_product_entity');
        /** @var string $columnIdentifier */
        $columnIdentifier = $this->entitiesHelper->getColumnIdentifier($table);
        /** @var array $values */
        $values = [
            'entity_id'        => '_entity_id',
            'attribute_set_id' => '_attribute_set_id',
            'type_id'          => '_type_id',
            'sku'              => 'identifier',
            'has_options'      => new Expr(0),
            'required_options' => new Expr(0),
            'updated_at'       => new Expr('now()'),
        ];

        if ($columnIdentifier == 'row_id') {
            $values['row_id'] = '_entity_id';
        }

        /** @var Select $parents */
        $parents = $connection->select()->from($tmpTable, $values);
        /** @var string $query */
        $query = $connection->insertFromSelect(
            $parents,
            $table,
            array_keys($values),
            AdapterInterface::INSERT_ON_DUPLICATE
        );
        $connection->query($query);

        $values = ['created_at' => new Expr('now()')];
        $connection->update($table, $values, 'created_at IS NULL');

        if ($columnIdentifier == 'row_id') {
            $values = [
                'created_in' => new Expr(1),
                'updated_in' => new Expr(VersionManager::MAX_VERSION),
            ];
            $connection->update($table, $values, 'created_in = 0 AND updated_in = 0');
        }
    }

    /**
     * Set values to attributes
     * Values are options applicable to a particular store.
     *
     * @return void
     * @throws LocalizedException
     */
    public function setValues()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var array $stores */
        $stores = $this->storeHelper->getAllStores();
        /** @var string[] $columns */
        $columns = array_keys($connection->describeTable($tmpTable));

        // Excluding irrelevant columns from processing.
        /** @var string[] $except */
        $except = [
            '_entity_id',
            '_is_new',
            '_status',
            '_type_id',
            '_options_container',
            '_tax_class_id',
            '_attribute_set_id',
            '_visibility',
            '_children',
            '_axis',
            'sku',
            'categories',
            'family',
            'groups',
            'parent',
            'enabled',
        ];

        // Beginning to map data for later db write.
        // The first item is a generic description.
        // The following items are store-specific.

        /** @var array $values */
        $values = [
            0 => [
                'options_container' => '_options_container',
                'tax_class_id'      => '_tax_class_id',
                'visibility'        => '_visibility',
            ],
        ];

        if ($connection->tableColumnExists($tmpTable, 'enabled')) {
            $values[0]['status'] = '_status';
        }

        /** @var array $taxClasses */
        $taxClasses = $this->configHelper->getProductTaxClasses();
        if (count($taxClasses)) {
            foreach ($taxClasses as $storeId => $taxClassId) {
                $values[$storeId]['tax_class_id'] = new Expr($taxClassId);
            }
        }

        /** @var string $column */
        foreach ($columns as $column) {
            if (in_array($column, $except) || preg_match('/-unit/', $column)) {
                continue;
            }

            // Some columns may be relevant only to particular stores.
            // Those have names patterned like so: prefix-suffix, where the suffix is the store name.

            /** @var array|string $columnPrefix */
            $columnPrefix = explode('-', $column);
            $columnPrefix = reset($columnPrefix);

            // For those columns, isolate the prefix and map it to the store in the values array.

            /**
             * @var string $suffix
             * @var array $affected
             */
            foreach ($stores as $suffix => $affected) {
                if (!preg_match('/^' . $columnPrefix . '-' . $suffix . '$/', $column)) {
                    continue;
                }

                /** @var array $store */
                foreach ($affected as $store) {
                    if (!isset($values[$store['store_id']])) {
                        $values[$store['store_id']] = [];
                    }
                    $values[$store['store_id']][$columnPrefix] = $column;
                }
            }

            if (!isset($values[0][$columnPrefix])) {
                $values[0][$columnPrefix] = $column;
            }
        }

        /** @var int $entityTypeId */
        $entityTypeId = $this->configHelper->getEntityTypeId(ProductModel::ENTITY);

        /**
         * @var string $storeId
         * @var array $data
         */
        foreach ($values as $storeId => $data) {
            $this->entitiesHelper->setValues(
                $this->getCode(),
                $connection->getTableName('catalog_product_entity'),
                $data,
                $entityTypeId,
                $storeId,
                AdapterInterface::INSERT_ON_DUPLICATE
            );
        }
    }

    /**
     * Link configurable with children
     *
     * @return void
     * @throws \Zend_Db_Statement_Exception
     * @throws LocalizedException
     */
    public function linkConfigurable()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());

        /** @var string|null $groupColumn */
        $groupColumn = null;
        if ($connection->tableColumnExists($tmpTable, 'parent')) {
            $groupColumn = 'parent';
        }
        if ($connection->tableColumnExists($tmpTable, 'groups') && !$groupColumn) {
            $groupColumn = 'groups';
        }
        if (!$groupColumn) {
            $this->setStatus(false);
            $this->setMessage(__('Columns groups or parent not found'));

            return;
        }

        /** @var Select $configurableSelect */
        $configurableSelect = $connection->select()
            ->from($tmpTable, ['_entity_id', '_axis','_children'])
            ->where('_type_id = ?','configurable')
            ->where('_axis IS NOT NULL')
            ->where('_children IS NOT NULL');

        /** @var int $stepSize */
        $stepSize = self::CONFIGURABLE_INSERTION_MAX_SIZE;
        /** @var array $valuesLabels */
        $valuesLabels = [];
        /** @var array $valuesRelations */
        $valuesRelations = []; // catalog_product_relation
        /** @var array $valuesSuperLink */
        $valuesSuperLink = []; // catalog_product_super_link
        /** @var Zend_Db_Statement_Pdo $query */
        $query = $connection->query($configurableSelect);
        /** @var array $stores */
        $stores = $this->storeHelper->getStores('store_id');

        /** @var array $row */
        while ($row = $query->fetch()) {
            if (!isset($row['_axis'])) {
                continue;
            }

            /** @var array $attributes */
            $attributes = explode(',', $row['_axis']);
            /** @var int $position */
            $position = 0;

            /** @var int $id */
            foreach ($attributes as $id) {
                if (!is_numeric($id) || !isset($row['_entity_id']) || !isset($row['_children'])) {
                    continue;
                }

                /** @var bool $hasOptions */
                $hasOptions = (bool)$connection->fetchOne(
                    $connection->select()
                        ->from($connection->getTableName('eav_attribute_option'), [new Expr(1)])
                        ->where('attribute_id = ?', $id)
                        ->limit(1)
                );

                if (!$hasOptions) {
                    continue;
                }

                /** @var array $values */
                $values = [
                    'product_id'   => $row['_entity_id'],
                    'attribute_id' => $id,
                    'position'     => $position++,
                ];
                $connection->insertOnDuplicate(
                    $connection->getTableName('catalog_product_super_attribute'),
                    $values,
                    []
                );

                /** @var string $superAttributeId */
                $superAttributeId = $connection->fetchOne(
                    $connection->select()
                        ->from($connection->getTableName('catalog_product_super_attribute'))
                        ->where('attribute_id = ?', $id)
                        ->where('product_id = ?', $row['_entity_id'])
                        ->limit(1)
                );

                /**
                 * @var int $storeId
                 * @var array $affected
                 */
                foreach ($stores as $storeId => $affected) {
                    $valuesLabels[] = [
                        'product_super_attribute_id' => $superAttributeId,
                        'store_id'                   => $storeId,
                        'use_default'                => 0,
                        'value'                      => '',
                    ];
                }

                /** @var array $children */
                $children = explode(',', $row['_children']);
                /** @var string $child */
                foreach ($children as $child) {
                    /** @var int $childId */
                    $childId = (int)$connection->fetchOne(
                        $connection->select()
                            ->from($connection->getTableName('catalog_product_entity'), ['entity_id'])
                            ->where('sku = ?', $child)
                            ->limit(1)
                    );

                    if (!$childId) {
                        continue;
                    }

                    $valuesRelations[] = [
                        'parent_id' => $row['_entity_id'],
                        'child_id'  => $childId,
                    ];

                    $valuesSuperLink[] = [
                        'product_id' => $childId,
                        'parent_id'  => $row['_entity_id'],
                    ];
                }

                if (count($valuesSuperLink) > $stepSize) {
                    $connection->insertOnDuplicate(
                        $connection->getTableName('catalog_product_super_attribute_label'),
                        $valuesLabels,
                        []
                    );

                    $connection->insertOnDuplicate(
                        $connection->getTableName('catalog_product_relation'),
                        $valuesRelations,
                        []
                    );

                    $connection->insertOnDuplicate(
                        $connection->getTableName('catalog_product_super_link'),
                        $valuesSuperLink,
                        []
                    );

                    $valuesLabels    = [];
                    $valuesRelations = [];
                    $valuesSuperLink = [];
                }
            }
        }

        if (count($valuesSuperLink) > 0) {
            $connection->insertOnDuplicate(
                $connection->getTableName('catalog_product_super_attribute_label'),
                $valuesLabels,
                []
            );

            $connection->insertOnDuplicate(
                $connection->getTableName('catalog_product_relation'),
                $valuesRelations,
                []
            );

            $connection->insertOnDuplicate(
                $connection->getTableName('catalog_product_super_link'),
                $valuesSuperLink,
                []
            );
        }
    }

    /**
     * Set website
     *
     * @return void
     * @throws LocalizedException
     */
    public function setWebsites()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var array $websites */
        $websites = $this->storeHelper->getStores('website_id');

        /**
         * @var int $websiteId
         * @var array $affected
         */
        foreach ($websites as $websiteId => $affected) {
            if ($websiteId == 0) {
                continue;
            }

            /** @var Select $select */
            $select = $connection->select()->from(
                    $tmpTable,
                    [
                        'product_id' => '_entity_id',
                        'website_id' => new Expr($websiteId),
                    ]
                );

            $connection->query(
                $connection->insertFromSelect(
                    $select,
                    $connection->getTableName('catalog_product_website'),
                    ['product_id', 'website_id'],
                    AdapterInterface::INSERT_ON_DUPLICATE
                )
            );
        }
    }

    /**
     * Set categories
     *
     * @return void
     */
    public function setCategories()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());

        if (!$connection->tableColumnExists($tmpTable, 'categories')) {
            $this->setStatus(false);
            $this->setMessage(__('Column categories not found'));

            return;
        }

        /** @var Select $select */
        $select = $connection->select()
            ->from(['c' => $connection->getTableName('pimgento_entities')], [])
            ->joinInner(
                ['p' => $tmpTable],
                'FIND_IN_SET(`c`.`code`, `p`.`categories`) AND `c`.`import` = "category"',
                [
                    'category_id' => 'c.entity_id',
                    'product_id'  => 'p._entity_id',
                ])
            ->joinInner(
                ['e' => $connection->getTableName('catalog_category_entity')],
                'c.entity_id = e.entity_id',
                []
            );

        $connection->query(
            $connection->insertFromSelect(
                $select,
                $connection->getTableName('catalog_category_product'),
                ['category_id', 'product_id'],
                1
            )
        );

        /** @var Select $selectToDelete */
        $selectToDelete = $connection->select()
            ->from(['c' => $connection->getTableName('pimgento_entities')], [])
            ->joinInner(
                ['p' => $tmpTable],
                '!FIND_IN_SET(`c`.`code`, `p`.`categories`) AND `c`.`import` = "category"',
                [
                    'category_id' => 'c.entity_id',
                    'product_id'  => 'p._entity_id',
                ])
            ->joinInner(
                ['e' => $connection->getTableName('catalog_category_entity')],
                'c.entity_id = e.entity_id',
                []
            );

        $connection->delete(
            $connection->getTableName('catalog_category_product'),
            '(category_id, product_id) IN (' . $selectToDelete->assemble() . ')'
        );
    }

    /**
     * Init stock
     *
     * @return void
     */
    public function initStock()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var int $websiteId */
        $websiteId = $this->configHelper->getDefaultScopeId();
        /** @var array $values */
        $values = [
            'product_id'                => '_entity_id',
            'stock_id'                  => new Expr(1),
            'qty'                       => new Expr(0),
            'is_in_stock'               => new Expr(0),
            'low_stock_date'            => new Expr('NULL'),
            'stock_status_changed_auto' => new Expr(0),
            'website_id'                => new Expr($websiteId),
        ];

        /** @var Select $select */
        $select = $connection->select()->from($tmpTable, $values);

        $connection->query(
            $connection->insertFromSelect(
                $select,
                $connection->getTableName('cataloginventory_stock_item'),
                array_keys($values),
                AdapterInterface::INSERT_IGNORE
            )
        );
    }

    /**
     * Update related, up-sell and cross-sell products
     *
     * @return void
     * @throws \Zend_Db_Exception
     */
    public function setRelated()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var string $entitiesTable */
        $entitiesTable = $connection->getTableName('pimgento_entities');
        /** @var string $productsTable */
        $productsTable = $connection->getTableName('catalog_product_entity');
        /** @var string $linkTable */
        $linkTable = $connection->getTableName('catalog_product_link');
        /** @var string $linkAttributeTable */
        $linkAttributeTable = $connection->getTableName('catalog_product_link_attribute');
        /** @var array $related */
        $related = [];

        /** @var string $columnIdentifier */
        $columnIdentifier = $this->entitiesHelper->getColumnIdentifier($productsTable);

        if ($connection->tableColumnExists($tmpTable, 'UPSELL-products')) {
            $related[Link::LINK_TYPE_UPSELL][] = '`p`.`UPSELL-products`';
        }
        if ($connection->tableColumnExists($tmpTable, 'UPSELL-product_models')) {
            $related[Link::LINK_TYPE_UPSELL][] = '`p`.`UPSELL-product_models`';
        }

        if ($connection->tableColumnExists($tmpTable, 'X_SELL-products')) {
            $related[Link::LINK_TYPE_CROSSSELL][] = '`p`.`X_SELL-products`';
        }
        if ($connection->tableColumnExists($tmpTable, 'X_SELL-product_models')) {
            $related[Link::LINK_TYPE_CROSSSELL][] = '`p`.`X_SELL-product_models`';
        }

        if ($connection->tableColumnExists($tmpTable, 'SUBSTITUTION-products')) {
            $related[Link::LINK_TYPE_RELATED][] = '`p`.`SUBSTITUTION-products`';
        }
        if ($connection->tableColumnExists($tmpTable, 'SUBSTITUTION-product_models')) {
            $related[Link::LINK_TYPE_RELATED][] = '`p`.`SUBSTITUTION-product_models`';
        }

        foreach ($related as $typeId => $columns) {
            $concat = 'CONCAT(' . join(',",",', $columns) . ')';
            $select = $connection->select()
                ->from(['c' => $entitiesTable], [])
                ->joinInner(
                    ['p' => $tmpTable],
                    'FIND_IN_SET(`c`.`code`, ' . $concat . ') AND
                        `c`.`import` = "' . $this->getCode() . '"',
                    [
                        'product_id'        => 'p._entity_id',
                        'linked_product_id' => 'c.entity_id',
                        'link_type_id'      => new Expr($typeId)
                    ]
                )
                ->joinInner(['e' => $productsTable], 'c.entity_id = e.' . $columnIdentifier, []);

            /* Remove old link */
            $connection->delete(
                $linkTable,
                ['(product_id, linked_product_id, link_type_id) NOT IN (?)' => $select, 'link_type_id = ?' => $typeId]
            );

            /* Insert new link */
            $connection->query(
                $connection->insertFromSelect(
                    $select,
                    $linkTable,
                    ['product_id', 'linked_product_id', 'link_type_id'],
                    AdapterInterface::INSERT_ON_DUPLICATE
                )
            );

            /* Insert position */
            $attributeId = $connection->fetchOne(
                $connection->select()
                    ->from($linkAttributeTable, ['product_link_attribute_id'])
                    ->where('product_link_attribute_code = ?', ProductLink::KEY_POSITION)
                    ->where('link_type_id = ?', $typeId)
            );

            if ($attributeId) {
                $select = $connection->select()
                    ->from($linkTable, [new Expr($attributeId), 'link_id', 'link_id'])
                    ->where('link_type_id = ?', $typeId);

                $connection->query(
                    $connection->insertFromSelect(
                        $select,
                        $connection->getTableName('catalog_product_link_attribute_int'),
                        ['product_link_attribute_id', 'link_id', 'value'],
                        AdapterInterface::INSERT_ON_DUPLICATE
                    )
                );
            }
        }
    }

    /**
     * Set Url Rewrite
     *
     * @return void
     * @throws LocalizedException
     * @throws \Zend_Db_Exception
     */
    public function setUrlRewrite()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tableName */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var array $stores */
        $stores = array_merge(
            $this->storeHelper->getStores(['lang']), // en_US
            $this->storeHelper->getStores(['lang', 'channel_code']) // en_US-channel
        );

        /**
         * @var string $local
         * @var array $affected
         */
        foreach ($stores as $local => $affected) {
            if (!$connection->tableColumnExists($tmpTable, 'url_key-' . $local)) {
                $connection->addColumn(
                    $tmpTable,
                    'url_key-' . $local,
                    [
                        'type' => 'text',
                        'length' => 255,
                        'default' => '',
                        'COMMENT' => ' ',
                        'nullable' => false
                    ]
                );
                $connection->update($tmpTable, ['url_key-' . $local => new Expr('`url_key`')]);
            }

            /**
             * @var array $affected
             * @var array $store
             */
            foreach ($affected as $store) {
                if (!$store['store_id']) {
                    continue;
                }
                /** @var \Magento\Framework\DB\Select $select */
                $select = $connection->select()
                    ->from(
                        $tmpTable,
                        [
                            'entity_id' => '_entity_id',
                            'url_key'   => 'url_key-' . $local,
                            'store_id'  => new Expr($store['store_id']),
                        ]
                    );

                /** @var \Magento\Framework\DB\Statement\Pdo\Mysql $query */
                $query = $connection->query($select);

                /** @var array $row */
                while (($row = $query->fetch())) {
                    /** @var ProductModel $product */
                    $product = $this->product;
                    $product->setData($row);

                    /** @var string $urlPath */
                    $urlPath = $this->productUrlPathGenerator->getUrlPath($product);

                    if (!$urlPath) {
                        continue;
                    }

                    /** @var string $requestPath */
                    $requestPath = $this->productUrlPathGenerator->getUrlPathWithSuffix(
                        $product,
                        $product->getStoreId()
                    );

                    /** @var string|null $exists */
                    $exists = $connection->fetchOne(
                        $connection->select()
                            ->from($connection->getTableName('url_rewrite'), new Expr(1))
                            ->where('entity_type = ?', ProductUrlRewriteGenerator::ENTITY_TYPE)
                            ->where('request_path = ?', $requestPath)
                            ->where('store_id = ?', $product->getStoreId())
                            ->where('entity_id <> ?', $product->getEntityId())
                    );
                    if ($exists) {
                        $product->setUrlKey($product->getUrlKey() . '-' . $product->getStoreId());
                        /** @var string $requestPath */
                        $requestPath = $this->productUrlPathGenerator->getUrlPathWithSuffix(
                            $product,
                            $product->getStoreId()
                        );
                    }

                    /** @var array $paths */
                    $paths = [
                        $requestPath => [
                            'request_path' => $requestPath,
                            'target_path'  => 'catalog/product/view/id/' . $product->getEntityId(),
                            'metadata'     => null,
                            'category_id'  => null,
                        ]
                    ];

                    /** @var bool $isCategoryUsedInProductUrl */
                    $isCategoryUsedInProductUrl = $this->configHelper->isCategoryUsedInProductUrl(
                        $product->getStoreId()
                    );

                    if ($isCategoryUsedInProductUrl) {
                        /** @var \Magento\Catalog\Model\ResourceModel\Category\Collection $categories */
                        $categories = $product->getCategoryCollection();
                        $categories->addAttributeToSelect('url_key');

                        /** @var CategoryModel $category */
                        foreach ($categories as $category) {
                            /** @var string $requestPath */
                            $requestPath = $this->productUrlPathGenerator->getUrlPathWithSuffix(
                                $product,
                                $product->getStoreId(),
                                $category
                            );
                            $paths[$requestPath] = [
                                'request_path' => $requestPath,
                                'target_path' => 'catalog/product/view/id/' . $product->getEntityId() .
                                    '/category/' . $category->getId(),
                                'metadata'     => '{"category_id":"' . $category->getId() . '"}',
                                'category_id'  => $category->getId(),
                            ];
                            $parents = $category->getParentCategories();
                            foreach ($parents as $parent) {
                                /** @var string $requestPath */
                                $requestPath = $this->productUrlPathGenerator->getUrlPathWithSuffix(
                                    $product,
                                    $product->getStoreId(),
                                    $parent
                                );
                                if (isset($paths[$requestPath])) {
                                    continue;
                                }
                                $paths[$requestPath] = [
                                    'request_path' => $requestPath,
                                    'target_path' => 'catalog/product/view/id/' . $product->getEntityId() .
                                        '/category/' . $parent->getId(),
                                    'metadata'     => '{"category_id":"' . $parent->getId() . '"}',
                                    'category_id'  => $parent->getId(),
                                ];
                            }
                        }
                    }

                    foreach ($paths as $path) {
                        if (!isset($path['request_path'], $path['target_path'])) {
                            continue;
                        }
                        /** @var string $requestPath */
                        $requestPath = $path['request_path'];
                        /** @var string $targetPath */
                        $targetPath = $path['target_path'];
                        /** @var string $metadata */
                        $metadata = $path['metadata'];

                        /** @var string|null $rewriteId */
                        $rewriteId = $connection->fetchOne(
                            $connection->select()
                                ->from($connection->getTableName('url_rewrite'), ['url_rewrite_id'])
                                ->where('entity_type = ?', ProductUrlRewriteGenerator::ENTITY_TYPE)
                                ->where('target_path = ?', $targetPath)
                                ->where('entity_id = ?', $product->getEntityId())
                                ->where('store_id = ?', $product->getStoreId())
                        );

                        if ($rewriteId) {
                            $connection->update(
                                $connection->getTableName('url_rewrite'),
                                ['request_path' => $requestPath, 'metadata' => $metadata],
                                ['url_rewrite_id = ?' => $rewriteId]
                            );
                        } else {
                            /** @var array $data */
                            $data = [
                                'entity_type' => ProductUrlRewriteGenerator::ENTITY_TYPE,
                                'entity_id' => $product->getEntityId(),
                                'request_path' => $requestPath,
                                'target_path' => $targetPath,
                                'redirect_type' => 0,
                                'store_id' => $product->getStoreId(),
                                'is_autogenerated' => 1,
                                'metadata' => $metadata,
                            ];

                            $connection->insertOnDuplicate(
                                $connection->getTableName('url_rewrite'),
                                $data,
                                array_keys($data)
                            );

                            if ($isCategoryUsedInProductUrl && $path['category_id']) {
                                /** @var int $rewriteId */
                                $rewriteId = $connection->fetchOne(
                                    $connection->select()
                                        ->from($connection->getTableName('url_rewrite'), ['url_rewrite_id'])
                                        ->where('entity_type = ?', ProductUrlRewriteGenerator::ENTITY_TYPE)
                                        ->where('target_path = ?', $targetPath)
                                        ->where('entity_id = ?', $product->getEntityId())
                                        ->where('store_id = ?', $product->getStoreId())
                                );
                            }
                        }

                        if ($isCategoryUsedInProductUrl && $rewriteId && $path['category_id']) {
                            $data = [
                                'url_rewrite_id' => $rewriteId,
                                'category_id'    => $path['category_id'],
                                'product_id'     => $product->getEntityId()
                            ];
                            $connection->delete(
                                $connection->getTableName('catalog_url_rewrite_product_category'),
                                ['url_rewrite_id = ?' => $rewriteId]
                            );
                            $connection->insertOnDuplicate(
                                $connection->getTableName('catalog_url_rewrite_product_category'),
                                $data,
                                array_keys($data)
                            );
                        }
                    }
                }
            }
        }
    }

    /**
     * Import the medias
     *
     * @return void
     */
    public function importMedia()
    {
        if (!$this->configHelper->isMediaImportEnabled()) {
            $this->setStatus(false);
            $this->setMessage(__('Media import is not enabled'));

            return;
        }

        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tableName */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var array $gallery */
        $gallery = $this->configHelper->getMediaImportGalleryColumns();

        if (empty($gallery)) {
            $this->setStatus(false);
            $this->setMessage(__('PIM Images Attributes is empty'));

            return;
        }

        /** @var string $table */
        $table = $connection->getTableName('catalog_product_entity');
        /** @var string $columnIdentifier */
        $columnIdentifier = $this->entitiesHelper->getColumnIdentifier($table);

        /** @var array $data */
        $data = [
            $columnIdentifier => '_entity_id',
            'sku'             => 'identifier',
        ];
        foreach ($gallery as $image) {
            if (!$connection->tableColumnExists($tmpTable, $image)) {
                $this->setMessage(__('Warning: %1 attribute does not exist', $image));
                continue;
            }
            $data[$image] = $image;
        }

        /** @var \Magento\Framework\DB\Select $select */
        $select = $connection->select()->from($tmpTable, $data);

        /** @var \Magento\Framework\DB\Statement\Pdo\Mysql $query */
        $query = $connection->query($select);

        /** @var \Magento\Catalog\Model\ResourceModel\Eav\Attribute $galleryAttribute */
        $galleryAttribute = $this->configHelper->getAttribute(ProductModel::ENTITY, 'media_gallery');
        /** @var string $galleryTable */
        $galleryTable = $connection->getTableName('catalog_product_entity_media_gallery');
        /** @var string $galleryEntityTable */
        $galleryEntityTable = $connection->getTableName('catalog_product_entity_media_gallery_value_to_entity');
        /** @var string $productImageTable */
        $productImageTable = $connection->getTableName('catalog_product_entity_varchar');

        /** @var array $row */
        while (($row = $query->fetch())) {
            /** @var array $files */
            $files = [];
            foreach ($gallery as $image) {
                if (!isset($row[$image])) {
                    continue;
                }

                if (!$row[$image]) {
                    continue;
                }

                /** @var array $media */
                $media = $this->akeneoClient->getProductMediaFileApi()->get($row[$image]);
                /** @var string $name */
                $name  = basename($media['code']);

                if (!$this->configHelper->mediaFileExists($name)) {
                    $binary = $this->akeneoClient->getProductMediaFileApi()->download($row[$image]);
                    $this->configHelper->saveMediaFile($name, $binary);
                }

                /** @var string $file */
                $file = $this->configHelper->getMediaFilePath($name);

                /** @var int $valueId */
                $valueId = $connection->fetchOne(
                    $connection->select()
                    ->from($galleryTable, ['value_id'])
                    ->where('value = ?', $file)
                );

                if (!$valueId) {
                    /** @var int $valueId */
                    $valueId = $connection->fetchOne(
                        $connection->select()->from($galleryTable, [new Expr('MAX(`value_id`)')])
                    );
                    $valueId += 1;
                }

                /** @var array $data */
                $data = [
                    'value_id'     => $valueId,
                    'attribute_id' => $galleryAttribute->getId(),
                    'value'        => $file,
                    'media_type'   => ImageEntryConverter::MEDIA_TYPE_CODE,
                    'disabled'     => 0,
                ];
                $connection->insertOnDuplicate($galleryTable, $data, array_keys($data));

                /** @var array $data */
                $data =  [
                    'value_id'        => $valueId,
                    $columnIdentifier => $row[$columnIdentifier]
                ];
                $connection->insertOnDuplicate($galleryEntityTable, $data, array_keys($data));

                /** @var array $columns */
                $columns = $this->configHelper->getMediaImportImagesColumns();

                foreach ($columns as $column) {
                    if ($column['column'] !== $image) {
                        continue;
                    }
                    /** @var array $data */
                    $data = [
                        'attribute_id'    => $column['attribute'],
                        'store_id'        => 0,
                        $columnIdentifier => $row[$columnIdentifier],
                        'value'           => $file
                    ];
                    $connection->insertOnDuplicate($productImageTable, $data, array_keys($data));
                }

                $files[] = $file;
            }

            /** @var \Magento\Framework\DB\Select $cleaner */
            $cleaner = $connection->select()
                ->from($galleryTable, ['value_id'])
                ->where('value NOT IN (?)', $files);

            $connection->delete(
                $galleryEntityTable,
                [
                    'value_id IN (?)'          => $cleaner,
                    $columnIdentifier . ' = ?' => $row[$columnIdentifier]
                ]
            );
        }
    }

    /**
     * Import the assets
     *
     * @return void
     */
    public function importAsset()
    {
        if (!$this->configHelper->isAkeneoEnterprise()) {
            $this->setStatus(false);
            $this->setMessage(__('Only available on Pim Enterprise'));

            return;
        }

        if (!$this->configHelper->isAssetImportEnabled()) {
            $this->setStatus(false);
            $this->setMessage(__('Asset import is not enabled'));

            return;
        }

        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tableName */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var array $gallery */
        $gallery = $this->configHelper->getAssetImportGalleryColumns();

        if (empty($gallery)) {
            $this->setStatus(false);
            $this->setMessage(__('PIM Asset Attributes is empty'));

            return;
        }

        /** @var string $table */
        $table = $connection->getTableName('catalog_product_entity');
        /** @var string $columnIdentifier */
        $columnIdentifier = $this->entitiesHelper->getColumnIdentifier($table);

        /** @var array $data */
        $data = [
            $columnIdentifier => '_entity_id',
            'sku'             => 'identifier',
        ];
        foreach ($gallery as $asset) {
            if (!$connection->tableColumnExists($tmpTable, $asset)) {
                $this->setMessage(__('Warning: %1 attribute does not exist', $asset));
                continue;
            }
            $data[$asset] = $asset;
        }

        /** @var \Magento\Framework\DB\Select $select */
        $select = $connection->select()->from($tmpTable, $data);

        /** @var \Magento\Framework\DB\Statement\Pdo\Mysql $query */
        $query = $connection->query($select);

        /** @var \Magento\Catalog\Model\ResourceModel\Eav\Attribute $galleryAttribute */
        $galleryAttribute = $this->configHelper->getAttribute(ProductModel::ENTITY, 'media_gallery');
        /** @var string $galleryTable */
        $galleryTable = $connection->getTableName('catalog_product_entity_media_gallery');
        /** @var string $galleryEntityTable */
        $galleryEntityTable = $connection->getTableName('catalog_product_entity_media_gallery_value_to_entity');
        /** @var string $galleryValueTable */
        $galleryValueTable = $connection->getTableName('catalog_product_entity_media_gallery_value');
        /** @var string $productImageTable */
        $productImageTable = $connection->getTableName('catalog_product_entity_varchar');

        /** @var array $row */
        while (($row = $query->fetch())) {
            /** @var array $files */
            $files = [];
            foreach ($gallery as $asset) {
                if (!isset($row[$asset])) {
                    continue;
                }

                if (!$row[$asset]) {
                    continue;
                }

                /** @var array $assets */
                $assets = explode(',', $row[$asset]);

                foreach ($assets as $key => $code) {
                    /** @var array $media */
                    $media = $this->akeneoClient->getAssetApi()->get($code);
                    if (!isset($media['code'], $media['reference_files'])) {
                        continue;
                    }

                    /** @var array $reference */
                    $reference = reset($media['reference_files']);
                    if (!$reference) {
                        continue;
                    }

                    /** @var string $name */
                    $name = basename($reference['code']);

                    if (!$this->configHelper->mediaFileExists($name)) {
                        if ($reference['locale']) {
                            $binary = $this->akeneoClient->getAssetReferenceFileApi()
                                ->downloadFromLocalizableAsset($media['code'], $reference['locale']);
                        } else {
                            $binary = $this->akeneoClient->getAssetReferenceFileApi()
                                ->downloadFromNotLocalizableAsset($media['code']);
                        }
                        $this->configHelper->saveMediaFile($name, $binary);
                    }

                    /** @var string $file */
                    $file = $this->configHelper->getMediaFilePath($name);

                    /** @var int $valueId */
                    $valueId = $connection->fetchOne(
                        $connection->select()
                            ->from($galleryTable, ['value_id'])
                            ->where('value = ?', $file)
                    );

                    if (!$valueId) {
                        /** @var int $valueId */
                        $valueId = $connection->fetchOne(
                            $connection->select()->from($galleryTable, [new Expr('MAX(`value_id`)')])
                        );
                        $valueId += 1;
                    }

                    /** @var array $data */
                    $data = [
                        'value_id' => $valueId,
                        'attribute_id' => $galleryAttribute->getId(),
                        'value' => $file,
                        'media_type' => ImageEntryConverter::MEDIA_TYPE_CODE,
                        'disabled' => 0,
                    ];
                    $connection->insertOnDuplicate($galleryTable, $data, array_keys($data));

                    /** @var array $data */
                    $data = [
                        'value_id'        => $valueId,
                        $columnIdentifier => $row[$columnIdentifier]
                    ];
                    $connection->insertOnDuplicate($galleryEntityTable, $data, array_keys($data));

                    /** @var array $data */
                    $data = [
                        'value_id'        => $valueId,
                        'store_id'        => 0,
                        $columnIdentifier => $row[$columnIdentifier],
                        'label'           => $media['description'],
                        'position'        => $key,
                        'disabled'        => 0,
                    ];
                    $connection->insertOnDuplicate($galleryValueTable, $data, array_keys($data));

                    if (empty($files)) {
                        /** @var array $entities */
                        $attributes = [
                            $this->configHelper->getAttribute(ProductModel::ENTITY, 'image'),
                            $this->configHelper->getAttribute(ProductModel::ENTITY, 'small_image'),
                            $this->configHelper->getAttribute(ProductModel::ENTITY, 'thumbnail'),
                        ];

                        foreach ($attributes as $attribute) {
                            if (!$attribute) {
                                continue;
                            }
                            /** @var array $data */
                            $data = [
                                'attribute_id'    => $attribute->getId(),
                                'store_id'        => 0,
                                $columnIdentifier => $row[$columnIdentifier],
                                'value'           => $file
                            ];
                            $connection->insertOnDuplicate($productImageTable, $data, array_keys($data));
                        }
                    }

                    $files[] = $file;
                }
            }

            /** @var \Magento\Framework\DB\Select $cleaner */
            $cleaner = $connection->select()
                ->from($galleryTable, ['value_id'])
                ->where('value NOT IN (?)', $files);

            $connection->delete(
                $galleryEntityTable,
                [
                    'value_id IN (?)'          => $cleaner,
                    $columnIdentifier . ' = ?' => $row[$columnIdentifier]
                ]
            );
        }
    }

    /**
     * Drop temporary table
     *
     * @return void
     */
    public function dropTable()
    {
        $this->entitiesHelper->dropTable($this->getCode());
        $this->entitiesHelper->dropTable($this->configurableTmpTableSuffix);
    }

    /**
     * Clean cache
     *
     * @return void
     */
    public function cleanCache()
    {
        /** @var array $types */
        $types = [
            Block::TYPE_IDENTIFIER,
            Type::TYPE_IDENTIFIER,
        ];

        /** @var string $type */
        foreach ($types as $type) {
            $this->cacheTypeList->cleanType($type);
        }

        $this->setMessage(__('Cache cleaned for: %1', join(', ', $types)));
    }
}
