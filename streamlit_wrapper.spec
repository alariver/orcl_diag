# -*- mode: python ; coding: utf-8 -*-


block_cipher = None


a = Analysis(['streamlit_wrapper.py'],
             pathex=['slt.py','queryUtil.py'],
             binaries=[],
             datas=[(
                     "/Users/changzhenghe/github/orcl_diag/lib/python3.8/site-packages/altair/vegalite/v4/schema/vega-lite-schema.json",
                     "./altair/vegalite/v4/schema/"
                 ),
                 (
                     "/Users/changzhenghe/github/orcl_diag/lib/python3.8/site-packages/streamlit/static",
                     "./streamlit/static"
                 ),
                 (
                     "/Users/changzhenghe/github/orcl_diag/lib/python3.8/site-packages/pyecharts/datasets",
                     "./pyecharts/datasets"
                 ),
                 (
                     "/Users/changzhenghe/github/orcl_diag/lib/python3.8/site-packages/streamlit_echarts",
                     "./streamlit_echarts"
                 )],
             hiddenimports=[],
             hookspath=['.'],
             hooksconfig={},
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)

exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,  
          [],
          name='streamlit_wrapper',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          upx_exclude=[],
          runtime_tmpdir=None,
          console=False,
          disable_windowed_traceback=False,
          target_arch=None,
          codesign_identity=None,
          entitlements_file=None )
app = BUNDLE(exe,
             name='streamlit_wrapper.app',
             icon=None,
             bundle_identifier=None)
