from PyQt6 import QtCore, QtGui, QtWidgets

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(1100, 640)
        font = QtGui.QFont()
        font.setPointSize(11)
        MainWindow.setFont(font)

        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.verticalRoot = QtWidgets.QVBoxLayout(self.centralwidget)

        self.tabWidget = QtWidgets.QTabWidget(self.centralwidget)

        # ===================== 主要功能頁 =====================
        self.tabMain = QtWidgets.QWidget()
        mainLay = QtWidgets.QVBoxLayout(self.tabMain)

        self.topSplitter = QtWidgets.QSplitter(self.tabMain)
        self.topSplitter.setOrientation(QtCore.Qt.Orientation.Horizontal)

        # -------- 左側：檔案與預覽 --------
        self.leftWidget = QtWidgets.QWidget(self.topSplitter)
        leftLay = QtWidgets.QVBoxLayout(self.leftWidget)

        self.grpFiles = QtWidgets.QGroupBox("檔案與預覽", self.leftWidget)
        gf = QtWidgets.QVBoxLayout(self.grpFiles)
        self.listFiles = QtWidgets.QListWidget(self.grpFiles)
        self.listFiles.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.ExtendedSelection)
        gf.addWidget(self.listFiles)
        rowBtns = QtWidgets.QHBoxLayout()
        self.btnRemoveSelected = QtWidgets.QPushButton("移除選取")
        self.btnClearList = QtWidgets.QPushButton("清空列表")
        self.btnRescanInput = QtWidgets.QPushButton("重新掃描 input")
        self.btnOpenOutput = QtWidgets.QPushButton("打開輸出資料夾")
        rowBtns.addWidget(self.btnRemoveSelected)
        rowBtns.addWidget(self.btnClearList)
        rowBtns.addStretch(1)
        rowBtns.addWidget(self.btnRescanInput)
        rowBtns.addWidget(self.btnOpenOutput)
        gf.addLayout(rowBtns)

        self.lblPreviewTitle = QtWidgets.QLabel("預覽 (最多顯示前 20 筆)")
        self.lblPreviewTitle.setStyleSheet("font-weight:600;")
        gf.addWidget(self.lblPreviewTitle)
        self.tablePreview = QtWidgets.QTableWidget(self.grpFiles)
        gf.addWidget(self.tablePreview)

        statusRow = QtWidgets.QHBoxLayout()
        self.lblStatus = QtWidgets.QLabel("DB:0 | v-")
        self.lblSummary = QtWidgets.QLabel("就緒")
        self.lblSummary.setStyleSheet("color:#444;")
        statusRow.addWidget(self.lblStatus)
        statusRow.addStretch(1)
        statusRow.addWidget(self.lblSummary)
        gf.addLayout(statusRow)

        self.progressBar = QtWidgets.QProgressBar(self.grpFiles)
        gf.addWidget(self.progressBar)

        leftLay.addWidget(self.grpFiles)
        self.topSplitter.addWidget(self.leftWidget)

        # -------- 右側：商品/任務/工具 --------
        self.rightWidget = QtWidgets.QWidget(self.topSplitter)
        rightLay = QtWidgets.QVBoxLayout(self.rightWidget)

        # 商品設定
        self.grpProduct = QtWidgets.QGroupBox("商品設定")
        gp = QtWidgets.QVBoxLayout(self.grpProduct)
        rowMode = QtWidgets.QHBoxLayout()
        rowMode.addWidget(QtWidgets.QLabel("模式："))
        self.cbProductMode = QtWidgets.QComboBox()
        self.cbProductMode.addItems(["單一商品", "隨機分布"])
        rowMode.addWidget(self.cbProductMode)
        gp.addLayout(rowMode)

        rowSingle = QtWidgets.QHBoxLayout()
        rowSingle.addWidget(QtWidgets.QLabel("單一商品："))
        self.cbSingleProduct = QtWidgets.QComboBox()
        self.cbSingleProduct.addItems(["遊戲幣", "遊戲寶物", "二手商品"])
        rowSingle.addWidget(self.cbSingleProduct)
        gp.addLayout(rowSingle)
        rightLay.addWidget(self.grpProduct)

        # 任務選擇
        self.grpTasks = QtWidgets.QGroupBox("批次處理任務")
        gt = QtWidgets.QVBoxLayout(self.grpTasks)
        self.chkTaskReport = QtWidgets.QCheckBox("更新媒合報表")
        self.chkTaskImages = QtWidgets.QCheckBox("生成聊天圖片")
        self.chkTaskWoo = QtWidgets.QCheckBox("轉換成可匯入網站格式")
        self.chkTaskUploadOrders = QtWidgets.QCheckBox("上傳網站並建立訂單")  # 第四選項（新）

        gt.addWidget(self.chkTaskReport)
        gt.addWidget(self.chkTaskImages)
        gt.addWidget(self.chkTaskWoo)
        gt.addWidget(self.chkTaskUploadOrders)

        self.btnRunAll = QtWidgets.QPushButton("執行所選任務")
        self.btnRunAll.setMinimumHeight(40)
        gt.addWidget(self.btnRunAll)
        rightLay.addWidget(self.grpTasks)

        # 工具 / 更新
        self.grpTools = QtWidgets.QGroupBox("工具 / 更新")
        gtools = QtWidgets.QVBoxLayout(self.grpTools)

        self.btnCheckUpdate = QtWidgets.QPushButton("檢查更新")
        self.btnCheckUpdate.setMinimumHeight(34)
        gtools.addWidget(self.btnCheckUpdate)

        miniRow = QtWidgets.QHBoxLayout()
        self.chkAutoClear = QtWidgets.QCheckBox("啟動自動清空")
        self.btnClearNow = QtWidgets.QPushButton("立即清空資料")
        miniRow.addWidget(self.chkAutoClear)
        miniRow.addWidget(self.btnClearNow)
        gtools.addLayout(miniRow)

        rightLay.addWidget(self.grpTools)
        rightLay.addStretch(1)

        self.topSplitter.addWidget(self.rightWidget)
        mainLay.addWidget(self.topSplitter)

        # 日誌
        self.grpLog = QtWidgets.QGroupBox("執行日誌")
        gl = QtWidgets.QVBoxLayout(self.grpLog)
        self.txtLog = QtWidgets.QPlainTextEdit(self.grpLog)
        self.txtLog.setReadOnly(True)
        self.txtLog.setMaximumHeight(110)
        gl.addWidget(self.txtLog)
        mainLay.addWidget(self.grpLog)

        self.tabWidget.addTab(self.tabMain, "主要功能")

        # ===================== 設定頁 =====================
        self.tabSettings = QtWidgets.QWidget()
        setLay = QtWidgets.QVBoxLayout(self.tabSettings)

        self.grpFeeSettings = QtWidgets.QGroupBox("平台手續費設定")
        gfs = QtWidgets.QVBoxLayout(self.grpFeeSettings)
        feeRow = QtWidgets.QHBoxLayout()
        feeRow.addWidget(QtWidgets.QLabel("手續費率 (%)："))
        self.spinFeeRate = QtWidgets.QDoubleSpinBox()
        self.spinFeeRate.setRange(0, 100)
        self.spinFeeRate.setDecimals(2)
        self.spinFeeRate.setValue(7.0)
        self.spinFeeRate.setSingleStep(0.1)
        self.spinFeeRate.setSuffix(" %")
        feeRow.addWidget(self.spinFeeRate)
        feeRow.addStretch(1)
        gfs.addLayout(feeRow)

        btnFeeRow = QtWidgets.QHBoxLayout()
        self.btnSaveSettings = QtWidgets.QPushButton("儲存手續費設定")
        self.btnResetSettings = QtWidgets.QPushButton("重設為預設")
        btnFeeRow.addWidget(self.btnSaveSettings)
        btnFeeRow.addWidget(self.btnResetSettings)
        btnFeeRow.addStretch(1)
        gfs.addLayout(btnFeeRow)

        setLay.addWidget(self.grpFeeSettings)
        setLay.addStretch(1)
        self.tabWidget.addTab(self.tabSettings, "設定")

        # ===================== WooCommerce 設定頁 =====================
        self.tabWoo = QtWidgets.QWidget()
        wooLay = QtWidgets.QVBoxLayout(self.tabWoo)
        self.grpWooSettings = QtWidgets.QGroupBox("WooCommerce 訂單連線設定")
        gws = QtWidgets.QVBoxLayout(self.grpWooSettings)

        rowUrl = QtWidgets.QHBoxLayout()
        rowUrl.addWidget(QtWidgets.QLabel("站點 URL："))
        self.editWooUrl = QtWidgets.QLineEdit()
        self.editWooUrl.setPlaceholderText("https://example.com")
        rowUrl.addWidget(self.editWooUrl)
        gws.addLayout(rowUrl)

        rowCK = QtWidgets.QHBoxLayout()
        rowCK.addWidget(QtWidgets.QLabel("Consumer Key："))
        self.editWooCK = QtWidgets.QLineEdit()
        rowCK.addWidget(self.editWooCK)
        gws.addLayout(rowCK)

        rowCS = QtWidgets.QHBoxLayout()
        rowCS.addWidget(QtWidgets.QLabel("Consumer Secret："))
        self.editWooCS = QtWidgets.QLineEdit()
        self.editWooCS.setEchoMode(QtWidgets.QLineEdit.EchoMode.Password)
        rowCS.addWidget(self.editWooCS)
        gws.addLayout(rowCS)

        rowFee = QtWidgets.QHBoxLayout()
        rowFee.addWidget(QtWidgets.QLabel("手續費商品 ID："))
        self.spinWooFeeProduct = QtWidgets.QSpinBox()
        self.spinWooFeeProduct.setRange(1, 999999999)
        self.spinWooFeeProduct.setValue(30977)
        rowFee.addWidget(self.spinWooFeeProduct)
        gws.addLayout(rowFee)

        rowBatch = QtWidgets.QHBoxLayout()
        rowBatch.addWidget(QtWidgets.QLabel("單次最大上傳筆數："))
        self.spinWooBatch = QtWidgets.QSpinBox()
        self.spinWooBatch.setRange(1, 2000)
        self.spinWooBatch.setValue(200)
        rowBatch.addWidget(self.spinWooBatch)
        gws.addLayout(rowBatch)

        rowTimeout = QtWidgets.QHBoxLayout()
        rowTimeout.addWidget(QtWidgets.QLabel("連線 Timeout(秒)："))
        self.spinWooTimeout = QtWidgets.QSpinBox()
        self.spinWooTimeout.setRange(3, 120)
        self.spinWooTimeout.setValue(15)
        rowTimeout.addWidget(self.spinWooTimeout)
        gws.addLayout(rowTimeout)

        rowRemote = QtWidgets.QHBoxLayout()
        rowRemote.addWidget(QtWidgets.QLabel("遠端重複掃描筆數："))
        self.spinWooRemoteScan = QtWidgets.QSpinBox()
        self.spinWooRemoteScan.setRange(10, 500)
        self.spinWooRemoteScan.setValue(200)
        rowRemote.addWidget(self.spinWooRemoteScan)
        gws.addLayout(rowRemote)

        rowWorkers = QtWidgets.QHBoxLayout()
        rowWorkers.addWidget(QtWidgets.QLabel("並行執行緒數："))
        self.spinWooWorkers = QtWidgets.QSpinBox()
        self.spinWooWorkers.setRange(1, 32)
        self.spinWooWorkers.setValue(6)
        rowWorkers.addWidget(self.spinWooWorkers)
        gws.addLayout(rowWorkers)

        self.chkWooSetCreated = QtWidgets.QCheckBox("使用原申請時間為訂單建立時間")
        self.chkWooTestMode = QtWidgets.QCheckBox("測試模式 (不建立實單)")
        gws.addWidget(self.chkWooSetCreated)
        gws.addWidget(self.chkWooTestMode)

        btnRow = QtWidgets.QHBoxLayout()
        self.btnWooSave = QtWidgets.QPushButton("儲存連線設定")
        self.btnWooTest = QtWidgets.QPushButton("測試連線")
        btnRow.addWidget(self.btnWooSave)
        btnRow.addWidget(self.btnWooTest)
        btnRow.addStretch(1)
        gws.addLayout(btnRow)

        self.lblWooStatus = QtWidgets.QLabel("尚未測試")
        self.lblWooStatus.setStyleSheet("color:#555;")
        gws.addWidget(self.lblWooStatus)

        wooLay.addWidget(self.grpWooSettings)
        wooLay.addStretch(1)
        self.tabWidget.addTab(self.tabWoo, "WooCommerce")

        self.verticalRoot.addWidget(self.tabWidget)
        MainWindow.setCentralWidget(self.centralwidget)

        self.menubar = QtWidgets.QMenuBar(MainWindow)
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        MainWindow.setStatusBar(self.statusbar)

    def retranslateUi(self, MainWindow):
        pass