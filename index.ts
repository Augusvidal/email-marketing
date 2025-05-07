import fs from "fs";
import csvParser from "csv-parser";
import path from "path";

sgMail.setApiKey(
    "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"
);

const FROM_EMAIL = "onboarding@resend.dev";
const FROM_NAME = "Augusto Vidal";
const HTML_TEMPLATE = fs.readFileSync(path.join(__dirname, "mail_edificio.html"), "utf-8");
const CONTACTS_FILE = "emailsmosca.csv";
const BATCH_SIZE = 1;
const ERROR_LOG_FILE = path.join(__dirname, "errors.txt");
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

interface Contact {
    email: string;
    name: string;
}

const readCsv = (filePath: string): Promise<Contact[]> => {
    return new Promise((resolve, reject) => {
        const results: Contact[] = [];
        fs.createReadStream(filePath)
            .pipe(csvParser({ 
                separator: ";",
                skipLines: 1,
                headers: ['name', 'email']
            }))
            .on("data", (data) => {
                if (data.email) {
                    results.push({
                        email: data.email.trim(),
                        name: data.name.trim()
                    });
                }
            })
            .on("end", () => resolve(results))
            .on("error", reject);
    });
};

const chunkArray = <T>(arr: T[], size: number): T[][] => {
    const result: T[][] = [];
    for (let i = 0; i < arr.length; i += size) {
        result.push(arr.slice(i, i + size));
    }
    return result;
};

const logErrorToFile = (
    batchNumber: number,
    contacts: Contact[],
    error: any
) => {
    const timestamp = new Date().toISOString();
    const errorEntry = `
[${timestamp}] ❌ Error en batch #${batchNumber}
→ Contactos:
${contacts.map((c) => `   - ${c.email}`).join("\n")}
→ Detalle: ${JSON.stringify(error, null, 2)}
------------------------------------------------------------\n`;

    fs.appendFileSync(ERROR_LOG_FILE, errorEntry, "utf-8");
};

const sendBatch = async (contacts: Contact[], batchNumber: number) => {
    try {
        console.log(contacts)
        const sendPromises = contacts.map(contact => 
            resend.emails.send({
                from: `${FROM_NAME} <${FROM_EMAIL}>`,
                to: contact.email,
                subject: "Tu asunto aquí",
                html: HTML_TEMPLATE
            })
        );

        await Promise.all(sendPromises);
        console.log(
            `✅ Batch #${batchNumber} enviado con ${contacts.length} correos.`
        );
    } catch (err: any) {
        console.error(
            `❌ Error en batch #${batchNumber}:`,
            err
        );
        logErrorToFile(batchNumber, contacts, err);
    }
};

const main = async () => {
    const contacts = await readCsv("nuevosEmails.csv"); // COLOCAR NOMBRE DEL ARCHIVO CSV ACA!!!
    const batches = chunkArray(contacts, BATCH_SIZE);
    
    console.log(`🚀 Iniciando envío de ${contacts.length} correos en ${batches.length} batches`);
    
    for (let i = 0; i < batches.length; i++) {
        await sendBatch(batches[i], i + 1);
        if (i < batches.length - 1) {
            console.log("⌛ Esperando 2 minutos antes del próximo batch...");
            await sleep(120000);
        }
    }

    console.log("🚀 Todos los correos enviados.");
};

main().catch(console.error);
