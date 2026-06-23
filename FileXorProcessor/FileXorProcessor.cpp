/**
 * @file FileXorProcessor.cpp
 * @brief XOR-модификация файлов по маске с фоновой обработкой, паузой и прогрессом.
 */

#include <atomic>

#include <QApplication>
#include <QCheckBox>
#include <QCloseEvent>
#include <QDateTime>
#include <QDir>
#include <QFile>
#include <QFileDialog>
#include <QFileInfo>
#include <QGridLayout>
#include <QGroupBox>
#include <QHBoxLayout>
#include <QLabel>
#include <QLineEdit>
#include <QMainWindow>
#include <QMessageBox>
#include <QMetaObject>
#include <QMutex>
#include <QMutexLocker>
#include <QProgressBar>
#include <QPushButton>
#include <QRadioButton>
#include <QRegularExpression>
#include <QSettings>
#include <QSpinBox>
#include <QTextEdit>
#include <QThread>
#include <QTimer>
#include <QVBoxLayout>
#include <QWaitCondition>

namespace {

const qint64 kChunkSize = 4 * 1024 * 1024;

enum class DuplicateAction
{
    Overwrite,
    CounterSuffix
};

enum class RunMode
{
    OneShot,
    Timer
};

struct ProcessorSettings
{
    QString inputDirectory;
    QString outputDirectory;
    QString fileMask;
    bool deleteSourceFiles = false;
    DuplicateAction duplicateAction = DuplicateAction::Overwrite;
    RunMode runMode = RunMode::OneShot;
    int pollIntervalSec = 5;
    QByteArray xorKey;
};

QString settingsFilePath()
{
    return QCoreApplication::applicationDirPath() + QStringLiteral("/filexor_settings.ini");
}

bool parseHexKey(const QString& text, QByteArray* key, QString* error)
{
    QString hex = text.trimmed();
    hex.remove(QLatin1Char(' '));
    hex.remove(QLatin1Char('-'));

    if (hex.size() != 16)
    {
        if (error)
        {
            *error = QStringLiteral("Ключ должен содержать ровно 16 hex-символов (8 байт).");
        }
        return false;
    }

    QByteArray result;
    result.reserve(8);
    for (int i = 0; i < hex.size(); i += 2)
    {
        bool ok = false;
        const int value = hex.mid(i, 2).toInt(&ok, 16);
        if (!ok)
        {
            if (error)
            {
                *error = QStringLiteral("Недопустимый символ в hex-ключе.");
            }
            return false;
        }
        result.append(static_cast<char>(value));
    }

    *key = result;
    return true;
}

QStringList splitMasks(const QString& maskText)
{
    QStringList masks = maskText.split(QRegularExpression(QStringLiteral("[;,]")), Qt::SkipEmptyParts);
    for (QString& mask : masks)
    {
        mask = mask.trimmed();
    }
    masks.removeAll(QString());
    return masks;
}

bool fileMatchesMask(const QString& fileName, const QString& mask)
{
    if (mask.contains(QLatin1Char('*')) || mask.contains(QLatin1Char('?')))
    {
        const QRegularExpression pattern(
            QRegularExpression::wildcardToRegularExpression(mask),
            QRegularExpression::CaseInsensitiveOption);
        return pattern.match(fileName).hasMatch();
    }
    return QString::compare(fileName, mask, Qt::CaseInsensitive) == 0;
}

bool fileMatchesAnyMask(const QString& fileName, const QStringList& masks)
{
    for (const QString& mask : masks)
    {
        if (fileMatchesMask(fileName, mask))
        {
            return true;
        }
    }
    return false;
}

QString resolveOutputPath(const QString& outputDirectory,
                          const QString& inputFilePath,
                          DuplicateAction duplicateAction)
{
    const QFileInfo inputInfo(inputFilePath);
    const QString baseName = inputInfo.completeBaseName();
    const QString suffix = inputInfo.suffix();
    const QString extension = suffix.isEmpty() ? QString() : QStringLiteral(".") + suffix;

    QString candidate = outputDirectory + QLatin1Char('/') + inputInfo.fileName();
    if (duplicateAction == DuplicateAction::Overwrite || !QFileInfo::exists(candidate))
    {
        return candidate;
    }

    int counter = 1;
    while (true)
    {
        candidate = outputDirectory + QLatin1Char('/')
            + baseName + QLatin1Char('_') + QString::number(counter) + extension;
        if (!QFileInfo::exists(candidate))
        {
            return candidate;
        }
        ++counter;
    }
}

void xorBuffer(QByteArray* buffer, qint64 globalOffset, const QByteArray& key)
{
    if (key.isEmpty())
    {
        return;
    }

    char* data = buffer->data();
    const int size = buffer->size();
    const int keySize = key.size();
    for (int i = 0; i < size; ++i)
    {
        data[i] = static_cast<char>(data[i] ^ key.at(static_cast<int>((globalOffset + i) % keySize)));
    }
}

} // namespace

class FileXorWorker : public QObject
{
    Q_OBJECT

public:
    explicit FileXorWorker(QObject* parent = nullptr)
        : QObject(parent)
    {
    }

public slots:
    void startProcessing(const ProcessorSettings& settings)
    {
        {
            QMutexLocker locker(&m_mutex);
            if (m_busy)
            {
                return;
            }
            m_busy = true;
            m_settings = settings;
            m_cancelRequested = false;
            m_pauseRequested = false;
            m_paused = false;
            m_active = true;
            if (m_resumeInputPath.isEmpty())
            {
                m_resumeOutputPath.clear();
                m_resumeOffset = 0;
                m_resumeTotalSize = 0;
            }
        }

        emit statusChanged(QStringLiteral("Сканирование файлов..."));
        processQueue(buildQueue(settings));
    }

    void pauseProcessing()
    {
        QMutexLocker locker(&m_mutex);
        m_pauseRequested = true;
    }

    void resumeProcessing()
    {
        QMutexLocker locker(&m_mutex);
        m_pauseRequested = false;
        m_paused = false;
        m_pauseCondition.wakeAll();
    }

    void stopProcessing()
    {
        {
            QMutexLocker locker(&m_mutex);
            m_cancelRequested = true;
            m_pauseRequested = false;
            m_paused = false;
            m_pauseCondition.wakeAll();
        }
        emit statusChanged(QStringLiteral("Остановка..."));
    }

signals:
    void progressChanged(qint64 processedBytes, qint64 totalBytes, const QString& fileName);
    void statusChanged(const QString& status);
    void logMessage(const QString& message);
    void fileFinished(const QString& inputPath, const QString& outputPath, bool success);
    void processingFinished(bool cancelled);
    void errorOccurred(const QString& message);

private:
    QStringList buildQueue(const ProcessorSettings& settings) const
    {
        QStringList queue;
        const QDir inputDir(settings.inputDirectory);
        if (!inputDir.exists())
        {
            return queue;
        }

        const QStringList masks = splitMasks(settings.fileMask);
        const QFileInfoList files = inputDir.entryInfoList(QDir::Files | QDir::NoDotAndDotDot, QDir::Name);
        for (const QFileInfo& fileInfo : files)
        {
            if (fileMatchesAnyMask(fileInfo.fileName(), masks))
            {
                queue.append(fileInfo.absoluteFilePath());
            }
        }
        return queue;
    }

    bool waitIfPaused()
    {
        QMutexLocker locker(&m_mutex);
        while (m_pauseRequested && !m_cancelRequested)
        {
            if (!m_paused)
            {
                m_paused = true;
                emit statusChanged(QStringLiteral("Пауза"));
            }
            m_pauseCondition.wait(&m_mutex, 200);
        }

        if (m_paused && !m_pauseRequested)
        {
            m_paused = false;
            emit statusChanged(QStringLiteral("Обработка"));
        }

        return m_cancelRequested;
    }

    bool processQueue(const QStringList& queue)
    {
        bool cancelled = false;

        for (const QString& inputPath : queue)
        {
            {
                QMutexLocker locker(&m_mutex);
                if (m_cancelRequested)
                {
                    cancelled = true;
                    break;
                }
            }

            const QString outputPath = processSingleFile(inputPath, QString());
            if (outputPath.isEmpty())
            {
                QMutexLocker locker(&m_mutex);
                if (m_cancelRequested)
                {
                    cancelled = true;
                }
                break;
            }

            emit fileFinished(inputPath, outputPath, true);
        }

        {
            QMutexLocker locker(&m_mutex);
            m_active = false;
            m_busy = false;
            if (!cancelled && m_cancelRequested)
            {
                cancelled = true;
            }
        }

        emit processingFinished(cancelled);
        return !cancelled;
    }

    QString processSingleFile(const QString& inputPath, const QString&)
    {
        QString outputPath;
        {
            QMutexLocker locker(&m_mutex);
            if (!m_resumeInputPath.isEmpty() && m_resumeInputPath == inputPath && !m_resumeOutputPath.isEmpty())
            {
                outputPath = m_resumeOutputPath;
            }
        }

        if (outputPath.isEmpty())
        {
            outputPath = resolveOutputPath(
                m_settings.outputDirectory,
                inputPath,
                m_settings.duplicateAction);
        }

        QFile inputFile(inputPath);
        if (!inputFile.open(QIODevice::ReadOnly))
        {
            emit errorOccurred(QStringLiteral("Не удалось открыть входной файл: %1").arg(inputPath));
            return QString();
        }

        const qint64 totalSize = inputFile.size();
        qint64 processedBytes = 0;

        {
            QMutexLocker locker(&m_mutex);
            if (!m_resumeInputPath.isEmpty() && m_resumeInputPath == inputPath)
            {
                processedBytes = m_resumeOffset;
                inputFile.seek(processedBytes);
            }
            else
            {
                m_resumeInputPath = inputPath;
                m_resumeOutputPath = outputPath;
                m_resumeOffset = 0;
                m_resumeTotalSize = totalSize;
            }
        }

        QFile outputFile(outputPath);
        QIODevice::OpenMode outputMode = QIODevice::WriteOnly;
        if (processedBytes > 0)
        {
            outputMode |= QIODevice::Append;
        }
        else if (m_settings.duplicateAction == DuplicateAction::Overwrite && QFileInfo::exists(outputPath))
        {
            QFile::remove(outputPath);
        }

        if (!outputFile.open(outputMode))
        {
            emit errorOccurred(QStringLiteral("Не удалось открыть выходной файл: %1").arg(outputPath));
            inputFile.close();
            return QString();
        }

        emit statusChanged(QStringLiteral("Обработка: %1").arg(QFileInfo(inputPath).fileName()));
        emit progressChanged(processedBytes, totalSize, QFileInfo(inputPath).fileName());
        emit logMessage(QStringLiteral("Начата обработка: %1 -> %2").arg(inputPath, outputPath));

        QByteArray buffer;
        buffer.resize(static_cast<int>(kChunkSize));

        while (processedBytes < totalSize)
        {
            if (waitIfPaused())
            {
                {
                    QMutexLocker locker(&m_mutex);
                    m_resumeOffset = processedBytes;
                }
                inputFile.close();
                outputFile.close();
                emit logMessage(QStringLiteral("Обработка прервана: %1 (смещение %2)")
                                    .arg(QFileInfo(inputPath).fileName())
                                    .arg(processedBytes));
                return QString();
            }

            const qint64 bytesToRead = qMin(kChunkSize, totalSize - processedBytes);
            const qint64 readBytes = inputFile.read(buffer.data(), bytesToRead);
            if (readBytes <= 0)
            {
                emit errorOccurred(QStringLiteral("Ошибка чтения файла: %1").arg(inputPath));
                inputFile.close();
                outputFile.close();
                return QString();
            }

            buffer.resize(static_cast<int>(readBytes));
            xorBuffer(&buffer, processedBytes, m_settings.xorKey);

            const qint64 writtenBytes = outputFile.write(buffer);
            if (writtenBytes != readBytes)
            {
                emit errorOccurred(QStringLiteral("Ошибка записи файла: %1").arg(outputPath));
                inputFile.close();
                outputFile.close();
                return QString();
            }

            processedBytes += readBytes;
            {
                QMutexLocker locker(&m_mutex);
                m_resumeOffset = processedBytes;
            }

            emit progressChanged(processedBytes, totalSize, QFileInfo(inputPath).fileName());
        }

        inputFile.close();
        outputFile.close();

        if (m_settings.deleteSourceFiles)
        {
            if (!QFile::remove(inputPath))
            {
                emit logMessage(QStringLiteral("Предупреждение: не удалось удалить исходный файл %1").arg(inputPath));
            }
        }

        {
            QMutexLocker locker(&m_mutex);
            m_resumeInputPath.clear();
            m_resumeOutputPath.clear();
            m_resumeOffset = 0;
            m_resumeTotalSize = 0;
        }

        emit logMessage(QStringLiteral("Готово: %1 (%2 байт)").arg(QFileInfo(outputPath).fileName()).arg(totalSize));
        return outputPath;
    }

    ProcessorSettings m_settings;
    QMutex m_mutex;
    QWaitCondition m_pauseCondition;
    bool m_cancelRequested = false;
    bool m_pauseRequested = false;
    bool m_paused = false;
    bool m_active = false;
    bool m_busy = false;
    QString m_resumeInputPath;
    QString m_resumeOutputPath;
    qint64 m_resumeOffset = 0;
    qint64 m_resumeTotalSize = 0;
};

class MainWindow : public QMainWindow
{
    Q_OBJECT

public:
    explicit MainWindow(QWidget* parent = nullptr)
        : QMainWindow(parent)
    {
        setWindowTitle(QStringLiteral("XOR-модификатор файлов"));
        setMinimumSize(760, 640);
        buildUi();
        loadSettings();
        updateControlState();

        m_worker = new FileXorWorker();
        m_worker->moveToThread(&m_workerThread);

        connect(&m_workerThread, &QThread::finished, m_worker, &QObject::deleteLater);
        connect(m_worker, &FileXorWorker::progressChanged, this, &MainWindow::onProgressChanged);
        connect(m_worker, &FileXorWorker::statusChanged, this, &MainWindow::onStatusChanged);
        connect(m_worker, &FileXorWorker::logMessage, this, &MainWindow::onLogMessage);
        connect(m_worker, &FileXorWorker::fileFinished, this, &MainWindow::onFileFinished);
        connect(m_worker, &FileXorWorker::processingFinished, this, &MainWindow::onProcessingFinished);
        connect(m_worker, &FileXorWorker::errorOccurred, this, &MainWindow::onErrorOccurred);

        m_workerThread.start();

        connect(m_pollTimer, &QTimer::timeout, this, &MainWindow::onPollTimer);
    }

    ~MainWindow() override
    {
        shutdownWorker();
    }

protected:
    void closeEvent(QCloseEvent* event) override
    {
        if (m_processingActive || m_shuttingDown)
        {
            if (!m_shuttingDown)
            {
                m_shuttingDown = true;
                m_processingActive = false;
                appendLog(QStringLiteral("Завершение работы, ожидание остановки обработки..."));
                m_pollTimer->stop();
                if (m_worker)
                {
                    QMetaObject::invokeMethod(m_worker, "stopProcessing", Qt::QueuedConnection);
                }
                m_closePending = true;
                event->ignore();
                return;
            }

            event->ignore();
            return;
        }

        saveSettings();
        shutdownWorker();
        event->accept();
    }

private:
    void buildUi()
    {
        QWidget* central = new QWidget(this);
        QVBoxLayout* root = new QVBoxLayout(central);

        QGroupBox* pathsGroup = new QGroupBox(QStringLiteral("Пути"), central);
        QGridLayout* pathsLayout = new QGridLayout(pathsGroup);

        m_inputDirEdit = new QLineEdit(pathsGroup);
        m_outputDirEdit = new QLineEdit(pathsGroup);
        QPushButton* inputBrowse = new QPushButton(QStringLiteral("Обзор..."), pathsGroup);
        QPushButton* outputBrowse = new QPushButton(QStringLiteral("Обзор..."), pathsGroup);

        pathsLayout->addWidget(new QLabel(QStringLiteral("Каталог поиска:"), pathsGroup), 0, 0);
        pathsLayout->addWidget(m_inputDirEdit, 0, 1);
        pathsLayout->addWidget(inputBrowse, 0, 2);
        pathsLayout->addWidget(new QLabel(QStringLiteral("Каталог результата:"), pathsGroup), 1, 0);
        pathsLayout->addWidget(m_outputDirEdit, 1, 1);
        pathsLayout->addWidget(outputBrowse, 1, 2);

        QGroupBox* filesGroup = new QGroupBox(QStringLiteral("Файлы"), central);
        QGridLayout* filesLayout = new QGridLayout(filesGroup);

        m_maskEdit = new QLineEdit(filesGroup);
        m_maskEdit->setPlaceholderText(QStringLiteral("*.txt, testFile.bin"));
        m_deleteSourceCheck = new QCheckBox(QStringLiteral("Удалять входные файлы после обработки"), filesGroup);
        m_overwriteRadio = new QRadioButton(QStringLiteral("Перезаписывать"), filesGroup);
        m_counterRadio = new QRadioButton(QStringLiteral("Добавлять счётчик к имени"), filesGroup);
        m_overwriteRadio->setChecked(true);

        filesLayout->addWidget(new QLabel(QStringLiteral("Маска файлов:"), filesGroup), 0, 0);
        filesLayout->addWidget(m_maskEdit, 0, 1, 1, 2);
        filesLayout->addWidget(m_deleteSourceCheck, 1, 0, 1, 3);
        filesLayout->addWidget(new QLabel(QStringLiteral("При совпадении имени:"), filesGroup), 2, 0);
        filesLayout->addWidget(m_overwriteRadio, 2, 1);
        filesLayout->addWidget(m_counterRadio, 2, 2);

        QGroupBox* modeGroup = new QGroupBox(QStringLiteral("Режим запуска"), central);
        QGridLayout* modeLayout = new QGridLayout(modeGroup);

        m_oneShotRadio = new QRadioButton(QStringLiteral("Разовый запуск"), modeGroup);
        m_timerRadio = new QRadioButton(QStringLiteral("По таймеру"), modeGroup);
        m_oneShotRadio->setChecked(true);
        m_pollIntervalSpin = new QSpinBox(modeGroup);
        m_pollIntervalSpin->setRange(1, 3600);
        m_pollIntervalSpin->setValue(5);
        m_pollIntervalSpin->setSuffix(QStringLiteral(" сек"));

        modeLayout->addWidget(m_oneShotRadio, 0, 0);
        modeLayout->addWidget(m_timerRadio, 0, 1);
        modeLayout->addWidget(new QLabel(QStringLiteral("Период опроса:"), modeGroup), 1, 0);
        modeLayout->addWidget(m_pollIntervalSpin, 1, 1);

        QGroupBox* keyGroup = new QGroupBox(QStringLiteral("XOR-ключ (8 байт, hex)"), central);
        QHBoxLayout* keyLayout = new QHBoxLayout(keyGroup);
        m_keyEdit = new QLineEdit(keyGroup);
        m_keyEdit->setPlaceholderText(QStringLiteral("1234567890ABCDEF"));
        m_keyEdit->setMaxLength(23);
        keyLayout->addWidget(m_keyEdit, 1);

        QHBoxLayout* buttonsLayout = new QHBoxLayout();
        m_startButton = new QPushButton(QStringLiteral("Старт"), central);
        m_pauseButton = new QPushButton(QStringLiteral("Пауза"), central);
        m_resumeButton = new QPushButton(QStringLiteral("Продолжить"), central);
        m_stopButton = new QPushButton(QStringLiteral("Стоп"), central);
        buttonsLayout->addWidget(m_startButton);
        buttonsLayout->addWidget(m_pauseButton);
        buttonsLayout->addWidget(m_resumeButton);
        buttonsLayout->addWidget(m_stopButton);
        buttonsLayout->addStretch();

        m_progressBar = new QProgressBar(central);
        m_progressBar->setRange(0, 100);
        m_statusLabel = new QLabel(QStringLiteral("Готов к работе"), central);
        m_logView = new QTextEdit(central);
        m_logView->setReadOnly(true);

        root->addWidget(pathsGroup);
        root->addWidget(filesGroup);
        root->addWidget(modeGroup);
        root->addWidget(keyGroup);
        root->addLayout(buttonsLayout);
        root->addWidget(m_progressBar);
        root->addWidget(m_statusLabel);
        root->addWidget(new QLabel(QStringLiteral("Журнал:"), central));
        root->addWidget(m_logView, 1);
        setCentralWidget(central);

        connect(inputBrowse, &QPushButton::clicked, this, &MainWindow::browseInputDir);
        connect(outputBrowse, &QPushButton::clicked, this, &MainWindow::browseOutputDir);
        connect(m_startButton, &QPushButton::clicked, this, &MainWindow::onStartClicked);
        connect(m_pauseButton, &QPushButton::clicked, this, &MainWindow::onPauseClicked);
        connect(m_resumeButton, &QPushButton::clicked, this, &MainWindow::onResumeClicked);
        connect(m_stopButton, &QPushButton::clicked, this, &MainWindow::onStopClicked);
        connect(m_timerRadio, &QRadioButton::toggled, this, &MainWindow::updateControlState);
    }

    void browseInputDir()
    {
        const QString dir = QFileDialog::getExistingDirectory(this, QStringLiteral("Каталог поиска"), m_inputDirEdit->text());
        if (!dir.isEmpty())
        {
            m_inputDirEdit->setText(dir);
        }
    }

    void browseOutputDir()
    {
        const QString dir = QFileDialog::getExistingDirectory(this, QStringLiteral("Каталог результата"), m_outputDirEdit->text());
        if (!dir.isEmpty())
        {
            m_outputDirEdit->setText(dir);
        }
    }

    bool collectSettings(ProcessorSettings* settings, QString* error) const
    {
        settings->inputDirectory = m_inputDirEdit->text().trimmed();
        settings->outputDirectory = m_outputDirEdit->text().trimmed();
        settings->fileMask = m_maskEdit->text().trimmed();
        settings->deleteSourceFiles = m_deleteSourceCheck->isChecked();
        settings->duplicateAction = m_counterRadio->isChecked()
            ? DuplicateAction::CounterSuffix
            : DuplicateAction::Overwrite;
        settings->runMode = m_timerRadio->isChecked() ? RunMode::Timer : RunMode::OneShot;
        settings->pollIntervalSec = m_pollIntervalSpin->value();

        if (settings->inputDirectory.isEmpty() || settings->outputDirectory.isEmpty())
        {
            *error = QStringLiteral("Укажите каталоги поиска и сохранения.");
            return false;
        }
        if (!QDir(settings->inputDirectory).exists())
        {
            *error = QStringLiteral("Каталог поиска не существует.");
            return false;
        }
        if (settings->fileMask.isEmpty())
        {
            *error = QStringLiteral("Укажите маску файлов.");
            return false;
        }
        if (!parseHexKey(m_keyEdit->text(), &settings->xorKey, error))
        {
            return false;
        }

        QDir().mkpath(settings->outputDirectory);
        return true;
    }

    void onStartClicked()
    {
        ProcessorSettings settings;
        QString error;
        if (!collectSettings(&settings, &error))
        {
            QMessageBox::warning(this, QStringLiteral("Настройки"), error);
            return;
        }

        m_currentSettings = settings;
        m_processingActive = true;
        m_paused = false;
        updateControlState();
        m_progressBar->setValue(0);
        appendLog(QStringLiteral("Запуск обработки..."));

        QMetaObject::invokeMethod(
            m_worker,
            "startProcessing",
            Qt::QueuedConnection,
            Q_ARG(ProcessorSettings, settings));

        if (settings.runMode == RunMode::Timer)
        {
            m_pollTimer->start(settings.pollIntervalSec * 1000);
        }
        else
        {
            m_pollTimer->stop();
        }
    }

    void onPauseClicked()
    {
        m_paused = true;
        QMetaObject::invokeMethod(m_worker, "pauseProcessing", Qt::QueuedConnection);
        updateControlState();
    }

    void onResumeClicked()
    {
        m_paused = false;
        QMetaObject::invokeMethod(m_worker, "resumeProcessing", Qt::QueuedConnection);
        updateControlState();
    }

    void onStopClicked()
    {
        m_pollTimer->stop();
        QMetaObject::invokeMethod(m_worker, "stopProcessing", Qt::QueuedConnection);
        updateControlState();
    }

    void onPollTimer()
    {
        if (!m_processingActive || m_paused)
        {
            return;
        }

        ProcessorSettings settings;
        QString error;
        if (!collectSettings(&settings, &error))
        {
            appendLog(error);
            return;
        }

        QMetaObject::invokeMethod(
            m_worker,
            "startProcessing",
            Qt::QueuedConnection,
            Q_ARG(ProcessorSettings, settings));
    }

    void onProgressChanged(qint64 processedBytes, qint64 totalBytes, const QString& fileName)
    {
        if (totalBytes > 0)
        {
            const int percent = static_cast<int>((processedBytes * 100) / totalBytes);
            m_progressBar->setValue(qBound(0, percent, 100));
        }
        m_statusLabel->setText(QStringLiteral("Файл %1: %2 / %3 байт")
                                 .arg(fileName)
                                 .arg(processedBytes)
                                 .arg(totalBytes));
    }

    void onStatusChanged(const QString& status)
    {
        if (!status.isEmpty())
        {
            m_statusLabel->setText(status);
        }
    }

    void onLogMessage(const QString& message)
    {
        appendLog(message);
    }

    void onFileFinished(const QString& inputPath, const QString& outputPath, bool success)
    {
        Q_UNUSED(inputPath);
        Q_UNUSED(outputPath);
        Q_UNUSED(success);
    }

    void onProcessingFinished(bool cancelled)
    {
        if (m_shuttingDown)
        {
            m_processingActive = false;
        }
        else if (m_currentSettings.runMode == RunMode::OneShot)
        {
            m_processingActive = false;
            m_pollTimer->stop();
        }

        if (cancelled)
        {
            appendLog(QStringLiteral("Обработка остановлена."));
            m_statusLabel->setText(QStringLiteral("Остановлено"));
        }
        else if (!m_shuttingDown && m_currentSettings.runMode == RunMode::OneShot)
        {
            appendLog(QStringLiteral("Обработка завершена."));
            m_statusLabel->setText(QStringLiteral("Готово"));
            m_progressBar->setValue(100);
        }
        else if (!m_shuttingDown && m_currentSettings.runMode == RunMode::Timer)
        {
            m_statusLabel->setText(QStringLiteral("Ожидание следующего опроса..."));
        }

        updateControlState();

        if (m_closePending)
        {
            m_processingActive = false;
            QTimer::singleShot(0, this, [this]() { close(); });
        }
    }

    void onErrorOccurred(const QString& message)
    {
        appendLog(QStringLiteral("Ошибка: %1").arg(message));
    }

    void appendLog(const QString& message)
    {
        m_logView->append(QStringLiteral("[%1] %2")
                              .arg(QDateTime::currentDateTime().toString(QStringLiteral("hh:mm:ss")))
                              .arg(message));
    }

    void updateControlState()
    {
        const bool timerMode = m_timerRadio->isChecked();
        m_pollIntervalSpin->setEnabled(timerMode);

        m_startButton->setEnabled(!m_processingActive || timerMode);
        m_pauseButton->setEnabled(m_processingActive && !m_paused);
        m_resumeButton->setEnabled(m_processingActive && m_paused);
        m_stopButton->setEnabled(m_processingActive);

        const bool editingEnabled = !m_processingActive;
        m_inputDirEdit->setEnabled(editingEnabled);
        m_outputDirEdit->setEnabled(editingEnabled);
        m_maskEdit->setEnabled(editingEnabled);
        m_deleteSourceCheck->setEnabled(editingEnabled);
        m_overwriteRadio->setEnabled(editingEnabled);
        m_counterRadio->setEnabled(editingEnabled);
        m_oneShotRadio->setEnabled(editingEnabled);
        m_timerRadio->setEnabled(editingEnabled);
        m_keyEdit->setEnabled(editingEnabled);
    }

    void loadSettings()
    {
        QSettings settings(settingsFilePath(), QSettings::IniFormat);
        m_inputDirEdit->setText(settings.value(QStringLiteral("paths/input")).toString());
        m_outputDirEdit->setText(settings.value(QStringLiteral("paths/output")).toString());
        m_maskEdit->setText(settings.value(QStringLiteral("files/mask"), QStringLiteral("*.bin")).toString());
        m_deleteSourceCheck->setChecked(settings.value(QStringLiteral("files/delete_source"), false).toBool());

        const QString duplicate = settings.value(QStringLiteral("files/duplicate"), QStringLiteral("overwrite")).toString();
        if (duplicate == QStringLiteral("counter"))
        {
            m_counterRadio->setChecked(true);
        }
        else
        {
            m_overwriteRadio->setChecked(true);
        }

        const QString runMode = settings.value(QStringLiteral("mode/run"), QStringLiteral("oneshot")).toString();
        if (runMode == QStringLiteral("timer"))
        {
            m_timerRadio->setChecked(true);
        }
        else
        {
            m_oneShotRadio->setChecked(true);
        }

        m_pollIntervalSpin->setValue(settings.value(QStringLiteral("mode/poll_sec"), 5).toInt());
        m_keyEdit->setText(settings.value(QStringLiteral("xor/key"), QStringLiteral("1234567890ABCDEF")).toString());
    }

    void saveSettings()
    {
        QSettings settings(settingsFilePath(), QSettings::IniFormat);
        settings.setValue(QStringLiteral("paths/input"), m_inputDirEdit->text().trimmed());
        settings.setValue(QStringLiteral("paths/output"), m_outputDirEdit->text().trimmed());
        settings.setValue(QStringLiteral("files/mask"), m_maskEdit->text().trimmed());
        settings.setValue(QStringLiteral("files/delete_source"), m_deleteSourceCheck->isChecked());
        settings.setValue(QStringLiteral("files/duplicate"), m_counterRadio->isChecked() ? QStringLiteral("counter") : QStringLiteral("overwrite"));
        settings.setValue(QStringLiteral("mode/run"), m_timerRadio->isChecked() ? QStringLiteral("timer") : QStringLiteral("oneshot"));
        settings.setValue(QStringLiteral("mode/poll_sec"), m_pollIntervalSpin->value());
        settings.setValue(QStringLiteral("xor/key"), m_keyEdit->text().trimmed());
    }

    void shutdownWorker()
    {
        m_pollTimer->stop();
        if (m_worker)
        {
            QMetaObject::invokeMethod(m_worker, "stopProcessing", Qt::QueuedConnection);
        }
        m_workerThread.quit();
        m_workerThread.wait(30000);
        m_worker = nullptr;
    }

    QLineEdit* m_inputDirEdit = nullptr;
    QLineEdit* m_outputDirEdit = nullptr;
    QLineEdit* m_maskEdit = nullptr;
    QLineEdit* m_keyEdit = nullptr;
    QCheckBox* m_deleteSourceCheck = nullptr;
    QRadioButton* m_overwriteRadio = nullptr;
    QRadioButton* m_counterRadio = nullptr;
    QRadioButton* m_oneShotRadio = nullptr;
    QRadioButton* m_timerRadio = nullptr;
    QSpinBox* m_pollIntervalSpin = nullptr;
    QPushButton* m_startButton = nullptr;
    QPushButton* m_pauseButton = nullptr;
    QPushButton* m_resumeButton = nullptr;
    QPushButton* m_stopButton = nullptr;
    QProgressBar* m_progressBar = nullptr;
    QLabel* m_statusLabel = nullptr;
    QTextEdit* m_logView = nullptr;
    QTimer* m_pollTimer = new QTimer(this);

    QThread m_workerThread;
    FileXorWorker* m_worker = nullptr;

    ProcessorSettings m_currentSettings;
    bool m_processingActive = false;
    bool m_paused = false;
    bool m_shuttingDown = false;
    bool m_closePending = false;
};

Q_DECLARE_METATYPE(ProcessorSettings)

int main(int argc, char* argv[])
{
    QApplication app(argc, argv);
    qRegisterMetaType<ProcessorSettings>();

    MainWindow window;
    window.show();

    return app.exec();
}

#include "FileXorProcessor.moc"
