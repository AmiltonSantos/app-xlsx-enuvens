const axios = require('axios');
const xlsx = require('xlsx');
require('dotenv').config();

const KEY_BEARER = process.env.KEY_BEARER;
const BASE_URL = process.env.BASE_URL;
const COD_GROUP = process.env.COD_GROUP;

// Função para buscar dados da API
async function fetchData() {
    try {

        // Busca os códigos dos membros da congregação
        const responseGroups = await axios.get(`${BASE_URL}/groups/${COD_GROUP}`, {
            headers: {
                Authorization: KEY_BEARER,
                Accept: 'application/json'
            }
        });

        const groupData = responseGroups.data.results;

        //Converter peoples (string → array)
        const peoples = JSON.parse(groupData.peoples);

        const rows = [];

        // (opcional) Cabeçalho do Excel
        rows.push([
            'Nome',
            'Função',
            'Batismo',
            'Foto',
            'Nacionalidade',
            'CPF',
            'Nascimento',
            'Naturalidade'
        ]);

        // Loop nas pessoas
        for (const personId of peoples) {

            const responsePeoples = await axios.get(`${BASE_URL}/people/${personId}`, {
                    headers: {
                        Authorization: KEY_BEARER,
                        Accept: 'application/json'
                    }
                }
            );

            const data = responsePeoples.data.results;

            // Extrafields
            const extrafields = JSON.parse(data.extrafields || '[]');
            const value15823 = extrafields.find(f => f.id_ef === 15823)?.value?.trim() || '';

            // Linha do Excel
            const cpf = data.doc_1?.replace(/^(\d{3})(\d{3})(\d{3})(\d{2})$/, '$1.$2.$3-$4').slice(0, 14);
            rows.push([
                data.full_name?.toUpperCase(),
                'MEMBRO',
                formatarData(data.baptism_date),
                cpf,
                'BRASILEIRA',
                cpf,
                formatarData(data.birthydate),
                value15823?.toUpperCase()
            ]);
        }

        // Gerar XLSX
        const ws = xlsx.utils.aoa_to_sheet(rows);

        // Definir largura das colunas
        const colunWidths = [35, 10, 10, 15, 15, 15, 10, 20];

        ws['!cols'] = colunWidths.map(width => ({ wch: width }));

        const wb = xlsx.utils.book_new();
        xlsx.utils.book_append_sheet(wb, ws, 'Data');

        xlsx.writeFile(wb, 'DADOS_ENUVENS.xlsx');

        console.log('Arquivo XLSX gerado com sucesso!');

    } catch (error) {
        console.error('Erro:', error.message);
    }
}

// Formata a data para o padrão brasileiro
function formatarData(data) {
  if (!data) return "";

  const [ano, mes, dia] = data.split("-");
  return `${dia}/${mes}/${ano}`;
}

// Chamar a função
fetchData();
