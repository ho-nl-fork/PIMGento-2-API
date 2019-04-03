<?php
namespace Pimgento\Api\Logger;

class Handler extends \Magento\Framework\Logger\Handler\Base
{
    protected $fileName = '/var/log/pimgento-import.log';
}