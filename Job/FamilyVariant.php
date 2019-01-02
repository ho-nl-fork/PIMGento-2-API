<?php

namespace Pimgento\Api\Job;

use Akeneo\Pim\ApiClient\Pagination\PageInterface;
use Akeneo\Pim\ApiClient\Pagination\ResourceCursorInterface;
use Magento\Catalog\Api\Data\ProductAttributeInterface;
use Magento\Eav\Model\Config;
use Magento\Framework\App\Cache\Type\Block as BlockCacheType;
use Magento\Framework\App\Cache\TypeListInterface;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\DB\Select;
use Magento\Framework\Event\ManagerInterface;
use Magento\PageCache\Model\Cache\Type as PageCacheType;
use Pimgento\Api\Helper\Authenticator;
use Pimgento\Api\Helper\Config as ConfigHelper;
use Pimgento\Api\Helper\Import\FamilyVariant as FamilyVariantHelper;
use Zend_Db_Expr as Expr;
use Pimgento\Api\Helper\Output as OutputHelper;

/**
 * Class FamilyVariant
 *
 * @category  Class
 * @package   Pimgento\Api\Job
 * @author    Agence Dn'D <contact@dnd.fr>
 * @copyright 2018 Agence Dn'D
 * @license   http://opensource.org/licenses/osl-3.0.php  Open Software License (OSL 3.0)
 * @link      https://www.pimgento.com/
 */
class FamilyVariant extends Import
{
    /**
     * @var int MAX_AXIS_NUMBER
     */
    const MAX_AXIS_NUMBER = 5;
    /**
     * @var string FAMILY_FORK_SUFFIX
     */
    const FAMILY_FORK_SUFFIX = '_fork';
    /**
     * @var string METRIC_UNIT_SUFFIX
     */
    const METRIC_UNIT_SUFFIX = '_unit';
    /**
     * @var string FORKED_ATTRIBUTE_TABLE_NAME
     */
    const FORKED_ATTRIBUTE_TABLE_NAME = 'pimgento_forked_attribute';
    /**
     * This variable contains a string value
     *
     * @var string $code
     */
    protected $code = 'family_variant';
    /**
     * This variable contains a string value
     *
     * @var string $name
     */
    protected $name = 'Family Variant';
    /**
     * This variable contains an FamilyVariantHelper
     *
     * @var FamilyVariantHelper $entitiesHelper
     */
    protected $entitiesHelper;
    /**
     * This variable contains a ConfigHelper
     *
     * @var ConfigHelper $configHelper
     */
    protected $configHelper;
    /**
     * This variable contains a TypeListInterface
     *
     * @var TypeListInterface $cacheTypeList
     */
    protected $cacheTypeList;
    /**
     * This variable contains a Config
     *
     * @var Config $eavConfig
     */
    protected $eavConfig;
    /**
     * @var Attribute $attributeJob
     */
    protected $attributeJob;


    /**
     * FamilyVariant constructor
     *
     * @param FamilyVariantHelper $entitiesHelper
     * @param ConfigHelper        $configHelper
     * @param OutputHelper        $outputHelper
     * @param ManagerInterface    $eventManager
     * @param Authenticator       $authenticator
     * @param TypeListInterface   $cacheTypeList
     * @param Config              $eavConfig
     * @param Attribute           $attributeJob
     * @param  \Psr\Log\LoggerInterface $logger
     * @param array               $data
     */
    public function __construct(
        FamilyVariantHelper $entitiesHelper,
        ConfigHelper $configHelper,
        OutputHelper $outputHelper,
        ManagerInterface $eventManager,
        Authenticator $authenticator,
        \Psr\Log\LoggerInterface $logger,
        TypeListInterface $cacheTypeList,
        Config $eavConfig,
        Attribute $attributeJob,
        array $data = []
    ) {
        parent::__construct($outputHelper, $eventManager, $authenticator, $logger, $data);

        $this->configHelper   = $configHelper;
        $this->entitiesHelper = $entitiesHelper;
        $this->cacheTypeList  = $cacheTypeList;
        $this->eavConfig      = $eavConfig;
        $this->attributeJob   = $attributeJob;
    }

    /**
     * Create temporary table
     *
     * @return void
     */
    public function createTable()
    {
        /** @var PageInterface $families */
        $families = $this->akeneoClient->getFamilyApi()->all(); // Querying the Family API all over again.
        /** @var bool $hasVariant */
        $hasVariant = false;
        /** @var array $family */
        foreach ($families as $family) {
            /** @var PageInterface $variantFamilies */
            $variantFamilies = $this->akeneoClient->getFamilyVariantApi()->listPerPage($family['code'], 1);
            if (count($variantFamilies->getItems()) > 0) {
                $hasVariant = true;

                break;
            }
        }
        if (!$hasVariant) {
            $this->setMessage(__('There is no family variant in Akeneo'));
            $this->stop();

            return;
        }
        /** @var array $variantFamily */
        $variantFamily = $variantFamilies->getItems(); // Grabbing the last item of $variantFamilies, as a sample.
        if (empty($variantFamily)) {
            $this->setMessage(__('No results retrieved from Akeneo'));
            $this->stop(1);

            return;
        }
        $variantFamily = reset($variantFamily);
        $this->entitiesHelper->createTmpTableFromApi($variantFamily, $this->getCode());
        // Created table: tmp_pimgento_entities_family_variant
    }

    /**
     * Insert data into temporary table
     *
     * @return void
     */
    public function insertData()
    {
        /** @var string|int $paginationSize */
        $paginationSize = $this->configHelper->getPanigationSize();
        /** @var PageInterface $families */
        $families = $this->akeneoClient->getFamilyApi()->all($paginationSize);
        /** @var int $count */
        $count = 0;
        /** @var array $family */
        foreach ($families as $family) {
            /** @var string $familyCode */
            $familyCode = $family['code'];
            $count      += $this->insertFamilyVariantData($familyCode, $paginationSize);
        }

        $this->setMessage(
            __('%1 line(s) found', $count)
        );
    }

    /**
     * Update Axis column
     *
     * @return void
     */
    public function updateAxis()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());

        // In tmp table, concatenate into a single _axis_code column
        // the contents of all the variant-axes_X columns

        $connection->addColumn($tmpTable, '_axis_codes', [
            'type' => 'text',
            'length' => 255,
            'default' => '',
            'COMMENT' => ' '
        ]);
        /** @var array $columns */
        $columns = [];
        /** @var int $i */
        for ($i = 1; $i <= self::MAX_AXIS_NUMBER; $i++) {
            $columns[] = 'variant-axes_'.$i;
        }
        /**
         * @var int    $key
         * @var string $column
         */
        foreach ($columns as $key => $column) {
            if (!$connection->tableColumnExists($tmpTable, $column)) {
                unset($columns[$key]);
            }
        }

        if (!empty($columns)) {
            /** @var string $update */
            $update = 'TRIM(BOTH "," FROM CONCAT(COALESCE(`' . join('`, \'\' ), "," , COALESCE(`', $columns) . '`, \'\')))';
            $connection->update($tmpTable, ['_axis_codes' => new Expr($update)]);
        }

        // Make an array of all _axis_codes present in the Family Variant tmp table, without duplicates.

        /** @var \Zend_Db_Statement_Interface $variantFamily */
        $variantFamily = $connection->query(
            $connection->select()->from($tmpTable)
        );
        /** @var string $allAxisCodesStr */
        $allAxisCodesStr = '';
        while ($row = $variantFamily->fetch()) {
            $allAxisCodesStr .= $row['_axis_codes'];
        }
        /** @var array $allAxisCodes */
        $allAxisCodesTmp = explode(',', $allAxisCodesStr);
        /** @var array $allAxisCodes */
        $allAxisCodes = array_unique($allAxisCodesTmp);

        // Query Attribute API to get all attributes whose type is "metric".
        // $metricAttributes contains K=>V pairs of the type ORIGINAL_ATTRIBUTE_CODE => ORIGINAL_ATTRIBUTE_AS_ARRAY

        $metricAttributes = [];
        $attributes = $this->akeneoClient->getAttributeApi()->all();
        foreach ($attributes as $attribute) {
            if ($attribute['type'] === 'pim_catalog_metric') {
                $metricAttributes[$attribute['code']] = $attribute;
            }
        }

        // Are any of the attributes identified as metric present in the Family Variant tmp table?
        // $metricAttributes contains K=>V pairs of the type ORIGINAL_ATTRIBUTE_CODE => FORKED_ATTRIBUTE_AS_ARRAY

        /** @var array $newAttributes */
        $newAttributes = [];
        foreach ($allAxisCodes as $code) {
            if (array_key_exists($code, $metricAttributes)) {

                // Assign those attributes a new code and type.

                $newAttribute = $metricAttributes[$code];
                $newAttribute['code'] = $code . self::FAMILY_FORK_SUFFIX;
                $newAttribute['type'] = 'pim_catalog_simpleselect';
                $newAttributes[$code] = $newAttribute;
            }
        }

        if (count($newAttributes)) {

            // Write those attributes to a new attribute tmp table
            $this->entitiesHelper->createTmpTableFromApi(reset($newAttributes), $this->attributeJob->getCode());
            foreach ($newAttributes as $index => $attribute) {
                $this->entitiesHelper->insertDataFromApi($attribute, $this->attributeJob->getCode());
            }

            // Process that table as an attribute job.
            $this->attributeJob->matchEntities();
            $this->attributeJob->matchType();
            $this->attributeJob->matchFamily();
            $this->attributeJob->addAttributes();
            $this->attributeJob->dropTable();

            // In the _axis_codes column of the tmp table, replace the codes of attributes that have been superseded.
            $variantFamily = $connection->query(
                $connection->select()->from($tmpTable)
            );
            while ($row = $variantFamily->fetch()) {
                /** @var array $rowCodes */
                $axisCodes = explode(',', $row['_axis_codes']);
                $newAxisCodes = [];
                foreach ($axisCodes as $code) {
                    if (array_key_exists($code, $metricAttributes)) {
                        $newAxisCodes[] = $code . self::FAMILY_FORK_SUFFIX;
                    } else {
                        $newAxisCodes[] = $code;
                    }
                }
                $connection->update($tmpTable, ['_axis_codes' => join(',', $newAxisCodes)], ['code = ?' => $row['code']]);
            }

            // Persist the codes of the attributes that required duplication.

            $connection = $this->entitiesHelper->getConnection();

            // Drop table if it exists.
            $connection->resetDdlCache(self::FORKED_ATTRIBUTE_TABLE_NAME);
            $connection->dropTable(self::FORKED_ATTRIBUTE_TABLE_NAME);

            // Create table.
            $forkedAttributeTable = $connection->newTable(self::FORKED_ATTRIBUTE_TABLE_NAME)
                ->addColumn('code', 'text')
                ->addColumn('code' . self::FAMILY_FORK_SUFFIX, 'text')
                ->addColumn('code' . self::METRIC_UNIT_SUFFIX, 'text');
            $connection->createTable($forkedAttributeTable);

            // Fetch all Measure Families from the API.
            $measureFamilies = $this->akeneoClient->getMeasureFamilyApi()->all();

            foreach ($newAttributes as $originalCode => $newAttribute) {

                // If in the PIM's "measure families" there is a symbol for this particular unit, use it.
                foreach ($measureFamilies as $measureFamily) {
                    foreach ($measureFamily['units'] as $unit) {
                        if ($newAttribute['default_metric_unit'] === $unit['code']) {
                            $newAttribute['default_metric_unit'] = $unit['symbol'];
                            break 2;
                        }
                    }
                }

                // Insert data.
                $connection->insertOnDuplicate(
                    $forkedAttributeTable->getName(),
                    [
                        'code'                              => $originalCode,
                        'code' . self::FAMILY_FORK_SUFFIX   => $newAttribute['code'],
                        'code' . self::METRIC_UNIT_SUFFIX   => $newAttribute['default_metric_unit']
                    ]);
            }
        }

        // In the tmp table, derive from the _axis_code column
        // an _axis column where variations are identified by their id.

        $connection->addColumn($tmpTable, '_axis', [
            'type' => 'text',
            'length' => 255,
            'default' => '',
            'COMMENT' => ' '
        ]);

        $select = $connection->select()->from(
            $connection->getTableName('eav_attribute'),
            ['attribute_code', 'attribute_id']
        )->where('entity_type_id = ?', $this->getEntityTypeId());
        /** @var array $attributes */
        $attributes = $connection->fetchPairs($select);

        /** @var \Zend_Db_Statement_Interface $variantFamily */
        $variantFamily = $connection->query(
            $connection->select()->from($tmpTable)
        );
        while ($row = $variantFamily->fetch()) {
            /** @var array $rowCodes */
            $axisCodes = explode(',', $row['_axis_codes']);
            /** @var array $axis */
            $axis = [];
            /** @var string $code */
            foreach ($axisCodes as $code) {
                if (isset($attributes[$code])) {
                    $axis[] = $attributes[$code];
                }
            }

            $connection->update($tmpTable, ['_axis' => join(',', $axis)], ['code = ?' => $row['code']]);
        }
    }

    /**
     * Update Product Model
     * The pimgento_product_model table is enriched with variation axes.
     *
     * @return void
     */
    public function updateProductModel()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var Select $query */
        $query = $connection->select()->from(false, ['axis' => 'f._axis'])->joinLeft(
            ['f' => $tmpTable],
            'p.family_variant = f.code',
            []
        );

        $connection->query(
            $connection->updateFromSelect($query, ['p' => $connection->getTableName('pimgento_product_model')])
        );
    }

    /**
     * Drop temporary table
     *
     * @return void
     */
    public function dropTable()
    {
//        $this->entitiesHelper->dropTable($this->getCode());
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
            BlockCacheType::TYPE_IDENTIFIER,
            PageCacheType::TYPE_IDENTIFIER,
        ];
        /** @var string $type */
        foreach ($types as $type) {
            $this->cacheTypeList->cleanType($type);
        }

        $this->setMessage(
            __('Cache cleaned for: %1', join(', ', $types))
        );
    }

    /**
     * Insert the FamilyVariant data in the temporary table for each family
     *
     * @param string $familyCode
     * @param int    $paginationSize
     *
     * @return int
     */
    protected function insertFamilyVariantData($familyCode, $paginationSize)
    {
        /** @var ResourceCursorInterface $families */
        $families = $this->akeneoClient->getFamilyVariantApi()->all($familyCode, $paginationSize);
        /**
         * @var int   $index
         * @var array $family
         */
        foreach ($families as $index => $family) {
            $this->entitiesHelper->insertDataFromApi($family, $this->getCode());
        }

        if (!isset($index)) {
            return 0;
        }
        $index++;

        return $index;
    }

    /**
     * Get the product entity type id
     *
     * @return string
     */
    protected function getEntityTypeId()
    {
        /** @var string $productEntityTypeId */
        $productEntityTypeId = $this->eavConfig->getEntityType(ProductAttributeInterface::ENTITY_TYPE_CODE)
            ->getEntityTypeId();

        return $productEntityTypeId;
    }
}
