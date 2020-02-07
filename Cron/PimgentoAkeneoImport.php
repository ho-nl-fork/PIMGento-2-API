<?php
/**
 * Copyright © Reach Digital (https://www.reachdigital.io/)
 * See LICENSE.txt for license details.
 */
declare(strict_types=1);

namespace Pimgento\Api\Cron;

use Magento\Framework\App\State;
use Magento\Framework\Phrase;
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
     * PimgentoAkeneoImport constructor.
     */
    public function __construct(
        ImportRepositoryInterface\Proxy $importRepository,
        State $appState,
        Logger $logger
    )
    {
        $this->appState         = $appState;
        $this->importRepository = $importRepository;
        $this->logger = $logger;
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

        // If the import is flagged as failed or incomplete, throw an error so the cronjob actually gets marked as
        // failed.
        if (!$import->getStatus()) {
            throw new \RuntimeException('One or more errors occured during import, see error log for more details: '.
                $import->getMessage());
        }

        return true;
    }
}
