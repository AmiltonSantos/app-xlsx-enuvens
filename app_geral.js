const axios = require('axios');
const xlsx = require('xlsx');
require('dotenv').config();

const KEY_BEARER = process.env.KEY_BEARER;
const BASE_URL = process.env.BASE_URL;
const BASE_URL_GROUPS = process.env.BASE_URL_GROUPS;

// Função para buscar dados da API
async function fetchData() {
    try {
        if (!BASE_URL_GROUPS) {
            throw new Error('BASE_URL_GROUPS não definida');
        }

        let contador = 0;
        const rows = [];

        // (opcional) Cabeçalho do Excel
        rows.push([
            'Congregacao',
            'Nome',
            'Pai',
            'Mãe',
            'CPF',
            'Nascimento',
            'Naturalidade',
            'Função',
            'Batismo',
            'Rua',
            'Número',
            'Bairro',
            'CEP',
            'Contato',
            'Data Cadastro'
        ]);

        const axiosHeaders = {
            headers: {
                Authorization: KEY_BEARER,
                Accept: 'application/json'
            }
        };

        const responseGroups = await axios.get(`${BASE_URL_GROUPS}`, axiosHeaders );

        const groupsData = responseGroups.data.results;

        // Loop nos grupos por congregacao
        for (const group of groupsData) {
            const nomeCongregacao = group?.name ?? 'N/A';

            // Busca os códigos dos membros da congregação
            const responseMembros = await axios.get(`${BASE_URL}/groups/${group.id}`, axiosHeaders );

            const membrosData = responseMembros.data.results;

            //Converter peoples (string → array)
            const peoples = JSON.parse(membrosData.peoples);

            if (peoples.length > 0) {
                // Criar um espaço entre uma congregacao e outra
                rows.push(['', '', '', '', '', '', '', '', '', '', '', '', '', '', '']);

                // Loop nas pessoas
                for (const personId of peoples) {
    
                    const responsePeoples = await axios.get(`${BASE_URL}/people/${personId}`, axiosHeaders );
    
                    const peopleData = responsePeoples.data.results;
    
                    if (peopleData) {
                        // Extrafields
                        const extrafields = JSON.parse(peopleData.extrafields || '[]');
                        const nomePai = extrafields.find(f => f.id_ef === 15819)?.value?.trim() || '';
                        const nomeMae = extrafields.find(f => f.id_ef === 15820)?.value?.trim() || '';
                        const naturalidade = extrafields.find(f => f.id_ef === 15823)?.value?.trim() || '';
                        const funcao = determinarFuncao(extrafields);
        
                        // Linha do Excel
                        const cpf = peopleData.doc_1?.replace(/^(\d{3})(\d{3})(\d{3})(\d{2})$/, '$1.$2.$3-$4').slice(0, 14);
                        rows.push([
                            nomeCongregacao.toUpperCase(),
                            peopleData.full_name?.toUpperCase(),
                            nomePai?.toUpperCase(),
                            nomeMae?.toUpperCase(),
                            cpf,
                            formatarData(peopleData.birthydate),
                            naturalidade?.toUpperCase(),
                            funcao,
                            formatarData(peopleData.baptism_date),
                            peopleData?.address_1?.toUpperCase() ?? '',
                            peopleData?.address_number ?? '',
                            peopleData?.address_2?.toUpperCase() ?? '',
                            peopleData?.postal_code,
                            peopleData.phone_1,
                            formatarData(peopleData.created_at)                  
                        ]);
                        contador ++;
                        console.log('QUANTIDADE DE REGISTRO: ', contador);
                    }
                }
            }
        }

        // Gerar XLSX
        const ws = xlsx.utils.aoa_to_sheet(rows);

        // Definir largura das colunas
        const colunWidths = [30, 35, 35, 35, 15, 10, 25, 20, 10, 40, 10, 30, 10, 15, 15];

        ws['!cols'] = colunWidths.map(width => ({ wch: width }));

        const wb = xlsx.utils.book_new();
        xlsx.utils.book_append_sheet(wb, ws, 'Data');

        xlsx.writeFile(wb, 'DADOS_ENUVENS.xlsx');

        console.log('Arquivo XLSX gerado com sucesso!');

    } catch (error) {
        if (error.code === 'ERR_INVALID_URL') {
            console.error('URL inválida:', BASE_URL_GROUPS);
        } else if (error.response) {
            console.error('Erro da API:', error.response.status, error.response.data);
        } else {
            console.error('Erro inesperado:', error.message);
        }
    
        // retorno seguro
        return null;
    }
}

// Formata a data para o padrão brasileiro
function formatarData(data) {
    if (!data) return "";

    const [ano, mes, dia] = data?.slice(0,10)?.split("-");
    return `${dia}/${mes}/${ano}`;
}

function determinarFuncao(extrafields) {
    const funcoes = [
        'MEMBRO',
        'COOPERADOR', 
        'DIÁCONO',
        'PRESBÍTERO',
        'EVANGELISTA',
        'PASTOR',
        'CONGREGADO'
    ];
    
    const campoFuncoes = extrafields.find(item => item.id_ef === "15822");
    
    if (!campoFuncoes || !campoFuncoes.sub) {
        return 'CAMPO NÃO ENCONTRADO';
    }
    
    const indexAtivo = campoFuncoes.sub.findIndex(sub => sub.value === true);
    
    return indexAtivo !== -1 ? funcoes[indexAtivo] : 'NENHUMA FUNÇÃO';
}

// Chamar a função
setTimeout(() => {
    fetchData();    
}, 5000);
