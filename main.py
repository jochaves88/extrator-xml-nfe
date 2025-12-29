import os
import shutil
import zipfile
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from sqlalchemy import create_engine, Column, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

app = FastAPI()

# --- CONFIGURAÇÃO DO BANCO ---
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./test.db")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class NFe(Base):
    __tablename__ = "notas_fiscais_v4" # V4 para resetar e garantir colunas novas
    chave_item = Column(String, primary_key=True, index=True)
    chave_acesso = Column(String)
    ano = Column(String)
    mes = Column(String)
    dados_json = Column(String)

Base.metadata.create_all(bind=engine)

# --- FUNÇÕES DE EXTRAÇÃO ---
ns_map = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

def get_val(node, path, type_fn=str):
    if node is None: return type_fn(0) if type_fn in [float, int] else ""
    try:
        r = node.find(path, ns_map)
        if r is None: r = node.find(path.replace('nfe:', '')) # Fallback sem namespace
        if r is not None and r.text:
            return type_fn(r.text.replace(',', '.'))
    except: pass
    return type_fn(0) if type_fn in [float, int] else ""

def parse_xml(filepath):
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
        
        # Ajuste para ler XMLs brutos ou processados
        if 'nfeProc' in root.tag: inf = root.find('.//nfe:infNFe', ns_map)
        else: inf = root.find('nfe:infNFe', ns_map)
        
        if inf is None: return []

        # Grupos Principais
        ide = inf.find('nfe:ide', ns_map)
        emit = inf.find('nfe:emit', ns_map)
        dest = inf.find('nfe:dest', ns_map)
        total = inf.find('.//nfe:ICMSTot', ns_map)
        
        # Chave e Data
        prot = root.find('.//nfe:infProt', ns_map)
        chave = get_val(prot, 'nfe:chNFe') or inf.attrib.get('Id', '')[3:]
        
        dt_str = get_val(ide, 'nfe:dhEmi') or get_val(ide, 'nfe:dEmi')
        dt = datetime.now()
        if len(dt_str) >= 10: dt = datetime.strptime(dt_str[:10], '%Y-%m-%d')

        # Totais da Nota (Cabeçalho)
        totais = {
            'vNF': get_val(total, 'nfe:vNF', float),
            'vBC': get_val(total, 'nfe:vBC', float),
            'vICMS': get_val(total, 'nfe:vICMS', float),
            'vBCST': get_val(total, 'nfe:vBCST', float),
            'vST': get_val(total, 'nfe:vST', float),
            'vProd': get_val(total, 'nfe:vProd', float),
            'vFrete': get_val(total, 'nfe:vFrete', float),
            'vSeg': get_val(total, 'nfe:vSeg', float),
            'vDesc': get_val(total, 'nfe:vDesc', float),
            'vIPI': get_val(total, 'nfe:vIPI', float),
            'vPIS': get_val(total, 'nfe:vPIS', float),
            'vCOFINS': get_val(total, 'nfe:vCOFINS', float),
            'vOutro': get_val(total, 'nfe:vOutro', float),
        }

        itens_db = []
        for i, det in enumerate(inf.findall('nfe:det', ns_map)):
            prod = det.find('nfe:prod', ns_map)
            imposto = det.find('nfe:imposto', ns_map)
            
            # Dados Completos para o Excel
            row = {
                # Identificação
                'Mês': str(dt.month).zfill(2),
                'Ano': str(dt.year),
                'Chave Acesso': chave,
                'Número NFe': get_val(ide, 'nfe:nNF'),
                'Série': get_val(ide, 'nfe:serie'),
                'Data Emissão': dt.strftime('%d/%m/%Y'),
                'Nat. Operação': get_val(ide, 'nfe:natOp'),
                
                # Empresas
                'Emitente Nome': get_val(emit, 'nfe:xNome'),
                'Emitente CNPJ': get_val(emit, 'nfe:CNPJ'),
                'Destinatário Nome': get_val(dest, 'nfe:xNome'),
                'Destinatário CNPJ': get_val(dest, 'nfe:CNPJ'),
                'UF': get_val(emit, 'nfe:enderEmit/nfe:UF'),
                
                # Totais da Nota (Repete em cada linha para facilitar filtro)
                'Vl Total NFe': totais['vNF'],
                'Vl Produtos (Nota)': totais['vProd'],
                'Vl Frete (Nota)': totais['vFrete'],
                'Vl Seguro (Nota)': totais['vSeg'],
                'Vl Desconto (Nota)': totais['vDesc'],
                'Vl Outras (Nota)': totais['vOutro'],
                'Vl IPI (Nota)': totais['vIPI'],
                'Vl PIS (Nota)': totais['vPIS'],
                'Vl COFINS (Nota)': totais['vCOFINS'],
                
                # Dados do PRODUTO (Item)
                'Cód. Prod': get_val(prod, 'nfe:cProd'),
                'Descrição Produto': get_val(prod, 'nfe:xProd'),
                'NCM': get_val(prod, 'nfe:NCM'),
                'CEST': get_val(prod, 'nfe:CEST'),
                'CFOP': get_val(prod, 'nfe:CFOP'),
                'Unidade': get_val(prod, 'nfe:uCom'),
                'Qtde': get_val(prod, 'nfe:qCom', float),
                'Vl Unitário': get_val(prod, 'nfe:vUnCom', float),
                'Vl Total Item': get_val(prod, 'nfe:vProd', float),
                
                # Impostos do Item (Específico deste produto)
                'ICMS Item': get_val(imposto, './/nfe:ICMS//nfe:vICMS', float),
                'IPI Item': get_val(imposto, './/nfe:IPI//nfe:vIPI', float),
                'PIS Item': get_val(imposto, './/nfe:PIS//nfe:vPIS', float),
                'COFINS Item': get_val(imposto, './/nfe:COFINS//nfe:vCOFINS', float),
            }
            
            itens_db.append(NFe(
                chave_item=f"{chave}-{i+1}", 
                chave_acesso=chave, 
                ano=str(dt.year),
                mes=str(dt.month).zfill(2),
                dados_json=str(row)
            ))
            
        return itens_db
    except Exception as e:
        print(f"Erro XML: {e}")
        return []

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
    except: return JSONResponse({"ok": False, "msg": "Arquivo deve ser .ZIP"})
    
    sess = SessionLocal()
    c = 0
    try:
        # Busca XMLs em todas as subpastas
        files = glob.glob(f"{tmp}/**/*.xml", recursive=True) + glob.glob(f"{tmp}/**/*.XML", recursive=True)
        for f in files:
            itens = parse_xml(f)
            for item in itens:
                sess.merge(item)
                c += 1
        sess.commit()
    except Exception as e: return JSONResponse({"ok": False, "msg": str(e)})
    finally: sess.close()
    
    return JSONResponse({"ok": True, "msg": f"{c} itens extraídos com sucesso!"})

@app.get("/anos")
async def anos():
    s = SessionLocal()
    # Ordena anos do banco
    a = s.query(NFe.ano).distinct().order_by(NFe.ano).all()
    s.close()
    return {"anos": [x[0] for x in a]}

@app.post("/relatorio")
async def relatorio(anos: str = Form(...)):
    s = SessionLocal()
    try:
        sel_anos = anos.split(',')
        res = s.query(NFe).filter(NFe.ano.in_(sel_anos)).all()
        
        if not res: return JSONResponse({"ok": False, "msg": "Sem dados para o período."})
        
        data = [eval(r.dados_json) for r in res]
        df = pd.DataFrame(data)
        
        # 1. ORDENAÇÃO (Ano, depois Mês)
        df = df.sort_values(by=['Ano', 'Mês'])
        
        # 2. DEFINIÇÃO DAS COLUNAS (Ordem Final do Excel)
        cols_final = [
            'Mês', 'Ano', 'Data Emissão', 'Número NFe', 'Série', 'Chave Acesso',
            'Emitente Nome', 'Emitente CNPJ', 'Destinatário Nome', 'Destinatário CNPJ', 'UF', 'Nat. Operação',
            'Cód. Prod', 'Descrição Produto', 'NCM', 'CEST', 'CFOP', 'Unidade', 'Qtde', 'Vl Unitário', 'Vl Total Item',
            'Vl Total NFe', 'Vl Produtos (Nota)', 'Vl Frete (Nota)', 'Vl Seguro (Nota)', 'Vl Desconto (Nota)', 
            'Vl Outras (Nota)', 'Vl IPI (Nota)', 'Vl PIS (Nota)', 'Vl COFINS (Nota)',
            'ICMS Item', 'IPI Item', 'PIS Item', 'COFINS Item'
        ]
        
        # Garante que todas colunas existam (preenche com vazio se não tiver)
        df = df.reindex(columns=cols_final).fillna("")
        
        df.to_excel("Relatorio_Completo.xlsx", index=False)
        
        # 3. CÁLCULO DA PROVA REAL
        v_itens = df['Vl Total Item'].replace('', 0).astype(float).sum()
        # Remove duplicadas de nota para somar o total da nota apenas uma vez
        df_unicas = df.drop_duplicates(subset=['Chave Acesso'])
        v_notas = df_unicas['Vl Total NFe'].replace('', 0).astype(float).sum()
        
        return JSONResponse({
            "ok": True,
            "notas": f"R$ {v_notas:,.2f}",
            "itens": f"R$ {v_itens:,.2f}",
            "qtd_notas": len(df_unicas),
            "qtd_linhas": len(df),
            "url": "/download"
        })
    finally: s.close()

@app.get("/download")
async def download():
    return FileResponse("Relatorio_Completo.xlsx", filename="Relatorio_NFe_Completo.xlsx")

# --- FRONTEND ---
@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <!DOCTYPE html>
    <html lang="pt-br">
    <head>
        <meta charset="UTF-8">
        <title>Extrator NFe Pro</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <script src="https://cdn.jsdelivr.net/npm/sweetalert2@11"></script>
        <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    </head>
    <body class="bg-gray-100 min-h-screen p-8">
        <div class="max-w-4xl mx-auto bg-white rounded-xl shadow-lg overflow-hidden">
            <div class="bg-blue-600 p-6 text-white text-center">
                <h1 class="text-2xl font-bold"><i class="fas fa-file-invoice"></i> Extrator Fiscal Completo</h1>
                <p class="text-blue-100 text-sm">Versão Final v4.0</p>
            </div>
            
            <div class="p-8 space-y-8">
                <div class="border-2 border-dashed border-gray-300 rounded-lg p-10 text-center hover:bg-blue-50 transition cursor-pointer" id="dropArea">
                    <i class="fas fa-cloud-upload-alt text-4xl text-blue-400 mb-2"></i>
                    <p class="text-gray-600 font-medium">Arraste seu ZIP aqui ou clique para buscar</p>
                    <input type="file" id="fileInput" accept=".zip" class="hidden">
                </div>

                <div>
                    <h3 class="font-bold text-gray-700 mb-3"><i class="fas fa-filter"></i> Selecione os Anos:</h3>
                    <div id="anosList" class="flex flex-wrap gap-2 text-sm text-gray-500">Carregando dados...</div>
                </div>

                <button id="btnGerar" onclick="gerarRelatorio()" disabled class="w-full bg-gray-300 text-gray-500 font-bold py-4 rounded-lg transition-all">
                    Aguardando seleção...
                </button>
            </div>
        </div>

        <script>
            const dropArea = document.getElementById('dropArea');
            const fileInput = document.getElementById('fileInput');
            
            // Upload Events
            dropArea.onclick = () => fileInput.click();
            fileInput.onchange = () => uploadFile(fileInput.files[0]);
            
            ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(evt => {
                dropArea.addEventListener(evt, (e) => { e.preventDefault(); e.stopPropagation(); });
            });
            dropArea.addEventListener('drop', (e) => uploadFile(e.dataTransfer.files[0]));

            async function uploadFile(file) {
                if(!file || !file.name.endsWith('.zip')) return Swal.fire('Erro', 'Envie um arquivo .ZIP', 'error');
                
                let fd = new FormData();
                fd.append('file', file);
                
                Swal.fire({title: 'Processando...', text: 'Lendo XMLs e salvando no banco...', allowOutsideClick: false, didOpen: () => Swal.showLoading()});
                
                try {
                    let res = await fetch('/upload', {method: 'POST', body: fd});
                    let data = await res.json();
                    if(data.ok) {
                        Swal.fire('Sucesso', data.msg, 'success');
                        loadAnos();
                    } else {
                        Swal.fire('Erro', data.msg, 'error');
                    }
                } catch(e) { Swal.fire('Erro', 'Falha na conexão', 'error'); }
            }

            async function loadAnos() {
                let res = await fetch('/anos');
                let data = await res.json();
                let div = document.getElementById('anosList');
                div.innerHTML = '';
                
                if(data.anos.length === 0) div.innerHTML = 'Nenhum dado encontrado.';
                
                data.anos.forEach(ano => {
                    div.innerHTML += `
                        <label class="cursor-pointer">
                            <input type="checkbox" value="${ano}" onchange="checkBtn()" class="peer sr-only">
                            <span class="px-4 py-2 rounded-full border peer-checked:bg-blue-600 peer-checked:text-white transition select-none hover:bg-gray-100">
                                ${ano}
                            </span>
                        </label>`;
                });
            }

            function checkBtn() {
                let count = document.querySelectorAll('input[type="checkbox"]:checked').length;
                let btn = document.getElementById('btnGerar');
                if(count > 0) {
                    btn.disabled = false;
                    btn.className = "w-full bg-blue-600 hover:bg-blue-700 text-white font-bold py-4 rounded-lg shadow-lg transition-all";
                    btn.innerHTML = "Gerar Relatório Excel";
                } else {
                    btn.disabled = true;
                    btn.className = "w-full bg-gray-300 text-gray-500 font-bold py-4 rounded-lg cursor-not-allowed";
                    btn.innerHTML = "Selecione um ano";
                }
            }

            async function gerarRelatorio() {
                let btn = document.getElementById('btnGerar');
                let anos = Array.from(document.querySelectorAll('input:checked')).map(x => x.value).join(',');
                
                btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Gerando...';
                
                let fd = new FormData();
                fd.append('anos', anos);
                
                try {
                    let res = await fetch('/relatorio', {method: 'POST', body: fd});
                    let data = await res.json();
                    
                    if(data.ok) {
                        Swal.fire({
                            title: 'Relatório Pronto!',
                            icon: 'success',
                            html: `
                                <div class="grid grid-cols-2 gap-4 text-left bg-gray-50 p-4 rounded mb-4">
                                    <div>
                                        <p class="text-xs text-gray-500 uppercase">Soma das Notas</p>
                                        <p class="text-xl font-bold text-green-600">${data.notas}</p>
                                        <p class="text-xs text-gray-400">${data.qtd_notas} notas</p>
                                    </div>
                                    <div>
                                        <p class="text-xs text-gray-500 uppercase">Soma dos Itens</p>
                                        <p class="text-xl font-bold text-blue-600">${data.itens}</p>
                                        <p class="text-xs text-gray-400">${data.qtd_linhas} itens</p>
                                    </div>
                                </div>
                            `,
                            confirmButtonText: 'Baixar Arquivo',
                            confirmButtonColor: '#16a34a'
                        }).then((result) => {
                            if(result.isConfirmed) window.location.href = data.url;
                        });
                    } else {
                        Swal.fire('Atenção', data.msg, 'warning');
                    }
                } catch(e) { Swal.fire('Erro', 'Falha ao gerar relatório', 'error'); }
                
                checkBtn();
            }
            
            loadAnos();
        </script>
    </body>
    </html>
    """
