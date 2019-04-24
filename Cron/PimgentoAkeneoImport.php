<?php
/**
 * Copyright © Reach Digital (https://www.reachdigital.io/)
 * See LICENSE.txt for license details.
 */
declare(strict_types=1);

namespace Pimgento\Api\Cron;

use Magento\Framework\App\State;
use Magento\Framework\Exception\NotFoundException;
use Magento\Framework\Phrase;
use Magento\MediaStorage\Service\ImageResize;
use Pimgento\Api\Api\ImportRepositoryInterface;
use Pimgento\Api\Job\Import;
use Pimgento\Api\Logger\Logger;
use \Symfony\Component\Console\Output\OutputInterface;

class PimgentoAkeneoImport
{
    /**
     * This variable contains a State
     *
     * @var State $appState
     */
    protected $appState;

    /**
     * This variable contains a ImportRepositoryInterface
     *
     * @var ImportRepositoryInterface $importRepository
     */
    private $importRepository;

    private $jobs = ['category', 'family', 'attribute', 'option', 'product_model', 'family_variant', 'product'];

    /**
     * @var Logger
     */
    private $logger;

    /**
     * @var ImageResize
     */
    private $resize;

    /**
     * PimgentoAkeneoImport constructor.
     */
    public function __construct(
        ImportRepositoryInterface\Proxy $importRepository,
        State $appState,
        Logger $logger,
        ImageResize $resize
    )
    {
        $this->appState         = $appState;
        $this->importRepository = $importRepository;
        $this->logger = $logger;
        $this->resize = $resize;
    }

    /**
     * Execute full import.
     *
     * @return $this
     */
    public function execute()
    {
        foreach ($this->jobs as $job) {
            if (!$this->import($job)) {
                break;
            }
        }

        // Resize images so that all imported images will appear in the cache.
        try {
            $generator = $this->resize->resizeFromThemes();
            while ($generator->valid()) {
                try {
                    $generator->next();
                } catch (\Exception $e) {
                    /** @var string $message */
                    $message = $e->getMessage();
                    $this->logger->error($message);
                }
            }
        } catch (NotFoundException $e) {
            /** @var string $message */
            $message = $e->getMessage();
            $this->logger->error($message);
        }
        return $this;
    }

    /**
     * Run import for specific part of the import.
     *
     * @param string $code
     *
     * @return bool
     */
    protected function import($code)
    {
        /** @var Import $import */
        $import = $this->importRepository->getByCode($code);
        if (!$import) {
            /** @var Phrase $message */
            $message = __('Import code not found');
            $this->logger->error('<error>' . $message . '</error>');

            return false;
        }

        try {
            $import->setStep(0);

            while ($import->canExecute()) {
                /** @var string $comment */
                $comment = $import->getComment();
                $this->logger->debug($comment);

                $import->execute();

                /** @var string $message */
                $message = $import->getMessage();
                if (!$import->getStatus()) {
                    $message = '<error>' . $message . '</error>';
                    $this->logger->error($message);
                }
                else {
                    $this->logger->debug($message);
                }

                if ($import->isDone()) {
                    break;
                }
            }

        } catch (\Exception $exception) {
            /** @var string $message */
            $message = $exception->getMessage();
            $this->logger->error($message);
            return false;
        }

        return true;
    }
}
