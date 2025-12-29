import os
import shutil
import zipfile
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from sqlalchemy import create_engine, Column, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

app = FastAPI()

# --- BANCO DE DADOS ---
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./test.db")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class NFe(Base):
    __tablename__ = "notas_fiscais_v3"  # V3 para garantir tabela limpa
    chave_item = Column(String, primary_key=True, index=True)
    chave_acesso = Column(String)
    ano = Column(String)
    dados_json = Column(String)

Base.metadata.create_all(bind=engine)

# --- FUNÇÕES XML ---
ns_map = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

def get_val(node, path, type_fn=str):
    if node is None: return type_fn(0) if type_fn in [float, int] else ""
    try:
        r = node.find(path, ns_map)
        if r is None: r = node.find(path.replace('nfe:', ''))
        if r is not None and r.text:
            return type_fn(r.text.replace(',', '.'))
    except: pass
    return type_fn(0) if type_fn in [float, int] else ""

def parse_xml(filepath):
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
        if 'nfeProc' in root.tag: inf = root.find('.//nfe:infNFe', ns_map)
        else: inf = root.find('nfe:infNFe', ns_map)
        if inf is None: return []

        ide = inf.find('nfe:ide', ns_map)
        emit = inf.find('nfe:emit', ns_map)
        dest = inf.find('nfe:dest', ns_map)
        total = inf.find('.//nfe:ICMSTot', ns_map)
        
        prot = root.find('.//nfe:infProt', ns_map)
        chave = get_val(prot, 'nfe:chNFe') or inf.attrib.get('Id', '')[3:]
        
        dt_str = get_val(ide, 'nfe:dhEmi') or get_val(ide, 'nfe:dEmi')
        dt = datetime.now()
        if len(dt_str) >= 10: dt = datetime.strptime(dt_str[:10], '%Y-%m-%d')

        v_nf = get_val(total, 'nfe:vNF', float)
        itens_db = []
        
        for i, det in enumerate(inf.findall('nfe:det', ns_map)):
            prod = det.find('nfe:prod', ns_map)
            v_prod = get_val(prod, 'nfe:vProd', float)
            
            row = {
                'Mês': str(dt.month).zfill(2),
                'Ano': str(dt.year),
                'Chave Acesso NFe': chave,
                'Inscrição Destinatário': get_val(dest, 'nfe:IE'),
                'Inscrição Emitente': get_val(emit, 'nfe:IE'),
                'Razão Social Emitente': get_val(emit, 'nfe:xNome'),
                'Cnpj Emitente': get_val(emit, 'nfe:CNPJ'),
                'UF Emitente': get_val(emit, 'nfe:enderEmit/nfe:UF'),
                'Nr NFe': get_val(ide, 'nfe:nNF'),
                'Série': get_val(ide, 'nfe:serie'),
                'Data NFe': dt.strftime('%d/%m/%Y'),
                'BC ICMS Total': get_val(total, 'nfe:vBC', float),
                'ICMS Total': get_val(total, 'nfe:vICMS', float),
                'BC ST Total': get_val(total, 'nfe:vBCST', float),
                'ICMS ST Total': get_val(total, 'nfe:vST', float),
                'Desc Total': get_val(total, 'nfe:vDesc', float),
                'IPI Total': get_val(total, 'nfe:vIPI', float),
                'Total Produtos': get_val(total, 'nfe:vProd', float),
                'Total NFe': v_nf,
                'Descrição Produto NFe': get_val(prod, 'nfe:xProd'),
                'NCM na NFe': get_val(prod, 'nfe:NCM'),
                'CFOP NFe': get_val(prod, 'nfe:CFOP'),
                'Qtde': get_val(prod, 'nfe:qCom', float),
                'Unid': get_val(prod, 'nfe:uCom'),
                'Vr Unit': get_val(prod, 'nfe:vUnCom', float),
                'Vr Total': v_prod,
                'Desconto Item': get_val(prod, 'nfe:vDesc', float)
            }
            itens_db.append(NFe(
                chave_item=f"{chave}-{i}", 
                chave_acesso=chave, 
                ano=str(dt.year), 
                dados_json=str(row)
            ))
        return itens_db
    except: return []

# --- ROTAS ---
@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    tmp = "temp_files"
    if os.path.exists(tmp): shutil.rmtree(tmp)
    os.makedirs(tmp)
    path = os.path.join(tmp, "upload.zip")
    with open(path, "wb") as b: shutil.copyfileobj(file.file, b)
    try:
        with zipfile.ZipFile(path, 'r') as z: z.extractall(tmp)
    except: return JSONResponse({"ok": False, "msg": "Arquivo inválido (use ZIP)"})
    
    sess = SessionLocal()
    c = 0
    try:
        for f in glob.glob(f"{tmp}/**/*.[xX][mM][lL]", recursive=True):
            for item in parse_xml(f):
                sess.merge(item)
                c += 1
        sess.commit()
    except Exception as e: return JSONResponse({"ok": False, "msg": str(e)})
    finally: sess.close()
    return JSONResponse({"ok": True, "msg": f"{c} itens processados."})

@app.get("/anos")
async def anos():
    s = SessionLocal()
    a = s.query(NFe.ano).distinct().order_by(NFe.ano).all()
    s.close()
    return {"anos": [x[0] for x in a]}

@app.post("/relatorio")
async def relatorio(anos: str = Form(...)):
    s = SessionLocal()
    try:
        res = s.query(NFe).filter(NFe.ano.in_(anos.split(','))).all()
        if not res: return JSONResponse({"ok": False, "msg": "Sem dados"})
        
        df = pd.DataFrame([eval(r.dados_json) for r in res])
        
        # --- ORDEM BLINDADA ---
        cols = [
            'Mês', 'Ano', 'Chave Acesso NFe', 'Inscrição Destinatário', 'Inscrição Emitente', 
            'Razão Social Emitente', 'Cnpj Emitente', 'UF Emitente', 'Nr NFe', 'Série', 'Data NFe', 
            'BC ICMS Total', 'ICMS Total', 'BC ST Total', 'ICMS ST Total', 'Desc Total', 'IPI Total', 
            'Total Produtos', 'Total NFe', 'Descrição Produto NFe', 'NCM na NFe', 'CFOP NFe', 
            'Qtde', 'Unid', 'Vr Unit', 'Vr Total', 'Desconto Item'
        ]
        # Reindex força a criação das colunas na ordem exata e preenche NaN com ""
        df = df.reindex(columns=cols).fillna("")
        
        out = "Relatorio.xlsx"
        df.to_excel(out, index=False)
        
        # Prova Real
        v_itens = df['Vr Total'].sum()
        v_notas = df.drop_duplicates('Chave Acesso NFe')['Total NFe'].sum()
        
        return JSONResponse({
            "ok": True,
            "notas": f"R$ {v_notas:,.2f}",
            "itens": f"R$ {v_itens:,.2f}",
            "qtd_notas": len(df.drop_duplicates('Chave Acesso NFe')),
            "qtd_linhas": len(df),
            "url": "/download"
        })
    finally: s.close()

@app.get("/download")
async def down(): return FileResponse("Relatorio.xlsx", filename="Relatorio_Final.xlsx")

# --- FRONTEND MODERNO (Tailwind + SweetAlert) ---
@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <!DOCTYPE html>
    <html lang="pt-br">
    <head>
        <meta charset="UTF-8">
        <title>Extrator Fiscal</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <script src="https://cdn.jsdelivr.net/npm/sweetalert2@11"></script>
        <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    </head>
    <body class="bg-slate-100 font-sans text-slate-800">
        <div class="max-w-4xl mx-auto p-6 space-y-8">
            
            <div class="text-center">
                <h1 class="text-3xl font-bold text-blue-600 mb-2"><i class="fas fa-file-invoice-dollar"></i> Extrator XML Pro</h1>
                <p class="text-slate-500">Importe suas notas e gere relatórios consolidados</p>
            </div>

            <div class="bg-white p-8 rounded-xl shadow-sm border border-slate-200">
                <h2 class="text-xl font-semibold mb-4 text-slate-700">1. Importação</h2>
                
                <div id="dropZone" class="border-4 border-dashed border-blue-200 rounded-xl p-10 text-center cursor-pointer hover:bg-blue-50 hover:border-blue-400 transition-all group">
                    <i class="fas fa-cloud-upload-alt text-5xl text-blue-300 group-hover:text-blue-500 mb-4"></i>
                    <p class="text-lg font-medium text-slate-600">Arraste seu arquivo ZIP aqui</p>
                    <p class="text-sm text-slate-400 mt-2">ou clique para selecionar</p>
                    <input type="file" id="fileInput" accept=".zip" class="hidden">
                </div>
                <div id="status" class="mt-4 text-center text-sm font-semibold h-6"></div>
            </div>

            <div class="bg-white p-8 rounded-xl shadow-sm border border-slate-200">
                <h2 class="text-xl font-semibold mb-4 text-slate-700">2. Relatório</h2>
                
                <div class="mb-4">
                    <p class="mb-2 text-sm text-slate-500 font-bold uppercase">Anos Disponíveis:</p>
                    <div id="yearsDiv" class="flex flex-wrap gap-3">
                        <span class="text-slate-400 italic">Carregando...</span>
                    </div>
                </div>

                <button id="btnGen" onclick="gerar()" disabled 
                    class="w-full py-4 bg-slate-300 text-white font-bold rounded-lg shadow-sm cursor-not-allowed transition-all">
                    Selecione um ano para gerar
                </button>
            </div>

        </div>

        <script>
            // --- DRAG AND DROP CORRIGIDO ---
            const dropZone = document.getElementById('dropZone');
            const fileInput = document.getElementById('fileInput');
            const status = document.getElementById('status');

            // Previne o navegador de abrir o arquivo
            ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(evt => {
                dropZone.addEventListener(evt, e => { e.preventDefault(); e.stopPropagation(); });
            });

            dropZone.addEventListener('dragover', () => dropZone.classList.add('bg-blue-50', 'border-blue-400'));
            dropZone.addEventListener('dragleave', () => dropZone.classList.remove('bg-blue-50', 'border-blue-400'));
            
            dropZone.addEventListener('drop', e => {
                dropZone.classList.remove('bg-blue-50', 'border-blue-400');
                if(e.dataTransfer.files.length) upload(e.dataTransfer.files[0]);
            });

            dropZone.addEventListener('click', () => fileInput.click());
            fileInput.addEventListener('change', () => { if(fileInput.files.length) upload(fileInput.files[0]); });

            async function upload(file) {
                if(file.type.indexOf('zip') === -1 && !file.name.endsWith('.zip')) {
                    Swal.fire('Erro', 'Por favor envie apenas arquivos .ZIP', 'error');
                    return;
                }
                
                status.innerHTML = '<span class="text-blue-600"><i class="fas fa-spinner fa-spin"></i> Enviando e processando...</span>';
                const fd = new FormData(); fd.append('file', file);
                
                try {
                    const res = await fetch('/upload', { method: 'POST', body: fd });
                    const data = await res.json();
                    
                    if(data.ok) {
                        status.innerHTML = `<span class="text-green-600"><i class="fas fa-check"></i> ${data.msg}</span>`;
                        loadYears();
                        Swal.fire('Sucesso!', data.msg, 'success');
                    } else { throw new Error(data.msg); }
                } catch(e) {
                    status.innerHTML = '<span class="text-red-500">Erro no envio.</span>';
                    Swal.fire('Erro', e.message, 'error');
                }
            }

            // --- FILTROS ---
            async function loadYears() {
                const res = await fetch('/anos');
                const data = await res.json();
                const div = document.getElementById('yearsDiv');
                div.innerHTML = '';
                
                if(data.anos.length === 0) {
                    div.innerHTML = '<span class="text-slate-400">Nenhum dado encontrado.</span>';
                    return;
                }

                data.anos.forEach(ano => {
                    div.innerHTML += `
                        <label class="cursor-pointer">
                            <input type="checkbox" value="${ano}" onchange="checkBtn()" class="peer sr-only">
                            <span class="px-4 py-2 bg-slate-100 border border-slate-300 rounded-full text-slate-600 peer-checked:bg-blue-600 peer-checked:text-white peer-checked:border-blue-600 transition-all hover:bg-slate-200 select-none">
                                ${ano}
                            </span>
                        </label>
                    `;
                });
            }

            function checkBtn() {
                const hasChecked = document.querySelectorAll('input[type="checkbox"]:checked').length > 0;
                const btn = document.getElementById('btnGen');
                if(hasChecked) {
                    btn.disabled = false;
                    btn.classList.remove('bg-slate-300', 'cursor-not-allowed');
                    btn.classList.add('bg-blue-600', 'hover:bg-blue-700', 'shadow-md');
                    btn.innerText = "Gerar Relatório e Ver Prova Real";
                } else {
                    btn.disabled = true;
                    btn.classList.add('bg-slate-300', 'cursor-not-allowed');
                    btn.classList.remove('bg-blue-600', 'hover:bg-blue-700', 'shadow-md');
                    btn.innerText = "Selecione um ano para gerar";
                }
            }

            // --- GERAÇÃO ---
            async function gerar() {
                const btn = document.getElementById('btnGen');
                const anos = Array.from(document.querySelectorAll('input:checked')).map(x => x.value).join(',');
                
                btn.innerHTML = '<i class="fas fa-circle-notch fa-spin"></i> Processando...';
                
                const fd = new FormData(); fd.append('anos', anos);
                
                try {
                    const res = await fetch('/relatorio', { method: 'POST', body: fd });
                    const data = await res.json();
                    
                    if(data.ok) {
                        // SWEET ALERT COM A PROVA REAL
                        Swal.fire({
                            title: '<strong>Relatório Gerado!</strong>',
                            icon: 'success',
                            html: `
                                <div class="grid grid-cols-2 gap-4 text-left bg-slate-50 p-4 rounded-lg border border-slate-200">
                                    <div>
                                        <div class="text-xs text-slate-500 uppercase font-bold">Total NFe</div>
                                        <div class="text-lg text-green-700 font-bold">${data.notas}</div>
                                        <div class="text-xs text-slate-400">${data.qtd_notas} notas</div>
                                    </div>
                                    <div>
                                        <div class="text-xs text-slate-500 uppercase font-bold">Total Produtos</div>
                                        <div class="text-lg text-green-700 font-bold">${data.itens}</div>
                                        <div class="text-xs text-slate-400">${data.qtd_linhas} itens</div>
                                    </div>
                                </div>
                            `,
                            showCancelButton: true,
                            confirmButtonText: '<i class="fas fa-download"></i> Baixar Excel',
                            cancelButtonText: 'Fechar',
                            confirmButtonColor: '#16a34a'
                        }).then((result) => {
                            if (result.isConfirmed) {
                                window.location.href = data.url;
                            }
                        });
                    } else {
                        Swal.fire('Atenção', data.msg, 'warning');
                    }
                } catch(e) {
                    Swal.fire('Erro', 'Falha na comunicação com o servidor.', 'error');
                }
                
                checkBtn(); // Reseta botão
            }
            
            // Inicia
            loadYears();
        </script>
    </body>
    </html>
    """
