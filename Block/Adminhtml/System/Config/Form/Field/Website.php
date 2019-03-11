<?php

namespace Pimgento\Api\Block\Adminhtml\System\Config\Form\Field;

use Magento\Config\Block\System\Config\Form\Field\FieldArray\AbstractFieldArray;
use Magento\Framework\Data\Form\Element\Factory;
use Magento\Backend\Block\Template\Context;

/**
 * Class Website
 *
 * @category  Class
 * @package   Pimgento\Api\Block\Adminhtml\System\Config\Form\Field
 * @author    Agence Dn'D <contact@dnd.fr>
 * @copyright 2018 Agence Dn'D
 * @license   http://opensource.org/licenses/osl-3.0.php  Open Software License (OSL 3.0)
 * @link      https://www.pimgento.com/
 */
class Website extends AbstractFieldArray
{
    /**
     * This variable contains a Factory
     *
     * @var Factory $elementFactory
     */
    protected $elementFactory;

    /**
     * Website constructor
     *
     * @param Context $context
     * @param Factory $elementFactory
     * @param array   $data
     */
    public function __construct(
        Context $context,
        Factory $elementFactory,
        array $data = []
    ) {
        parent::__construct($context, $data);

        $this->elementFactory = $elementFactory;
    }

    /**
     * Initialise form fields
     *
     * @return void
     */
    protected function _construct()
    {
        $this->addColumn(
            'website',
            [
                'label' => __('Website'),
            ]
        );
        $this->addColumn(
            'channel',
            [
                'label' => __('Channel'),
                'class' => 'required-entry',
            ]
        );
        $this->_addAfter       = false;
        $this->_addButtonLabel = __('Add');

        parent::_construct();
    }

    /**
     * Render array cell for prototypeJS template
     *
     * @param string $columnName
     *
     * @return string
     */
    public function renderCellTemplate($columnName)
    {
        if ($columnName !== 'website') {
            return parent::renderCellTemplate($columnName);
        }

        /** @var \Magento\Store\Api\Data\WebsiteInterface[] $websites */
        $websites = $this->_storeManager->getWebsites();
        /** @var array $options */
        $options = [];
        /** @var \Magento\Store\Api\Data\WebsiteInterface $website */
        foreach ($websites as $website) {
            $options[$website->getCode()] = $website->getCode();
        }

        /** @var \Magento\Framework\Data\Form\Element\Select $element */
        $element = $this->elementFactory->create('select');
        $element->setForm(
            $this->getForm()
        )->setName(
            $this->_getCellInputElementName($columnName)
        )->setHtmlId(
            $this->_getCellInputElementId('<%- _id %>', $columnName)
        )->setValues(
            $options
        );

        return str_replace("\n", '', $element->getElementHtml());
    }
}
