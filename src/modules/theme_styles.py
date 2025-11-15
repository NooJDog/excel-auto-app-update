ENHANCED_QSS = """
QMainWindow { background: #eef2f6; }
QGroupBox {
  background: #ffffff;
  border: 1px solid #d8dee4;
  border-radius: 6px;
  margin-top: 16px;
  padding-top: 10px;
  font-weight: 600;
}
QGroupBox::title {
  subcontrol-origin: margin;
  subcontrol-position: top left;
  padding: 4px 8px;
  background: #ffffff;
  border-radius: 4px;
}
QPushButton {
  background: #2563eb;
  color: #fff;
  border: 1px solid #2563eb;
  border-radius: 5px;
  padding: 6px 14px;
  font-weight: 500;
}
QPushButton:hover { background: #1d4ed8; }
QPushButton:disabled { background: #94a3b8; border-color: #94a3b8; }
QPushButton.danger { background: #dc2626; border-color: #dc2626; }
QPushButton.danger:hover { background: #b91c1c; }
QCheckBox { spacing: 6px; font-weight: 500; }
QListWidget {
  background: #ffffff;
  border: 1px solid #d0d7de;
  border-radius: 4px;
}
QListWidget::item { padding: 4px; }
QListWidget::item:selected { background: #2563eb; color: #fff; }
QListWidget::item:alternate { background: #f1f5f9; }
QProgressBar {
  border: 1px solid #d0d7de;
  border-radius: 4px;
  text-align: center;
}
QProgressBar::chunk { background-color: #409eff; border-radius: 4px; }
"""