import fs from "fs";
import csvParser from "csv-parser";
import sgMail from "@sendgrid/mail";
import path from "path";

sgMail.setApiKey(
    "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"
);

const TEMPLATE_ID = "d-a1b3089e7217410bb7847c3803ca3d9c";
const FROM_EMAIL = "zuly.vazquez@atlas.red";
const BATCH_SIZE = 500;
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
            .pipe(csvParser({ separator: ";", skipLines: 0 }))
            .on("data", (data) => {
                // Validación: si hay nombre pero no hay email, lo salteamos
                if (data.email)
                    results.push({ email: data.email, name: data.name });
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
→ Detalle: ${JSON.stringify(error.response?.body || error.message, null, 2)}
------------------------------------------------------------\n`;

    fs.appendFileSync(ERROR_LOG_FILE, errorEntry, "utf-8");
};

const sendBatch = async (contacts: Contact[], batchNumber: number) => {
    const messages = contacts.map((c) => ({
        to: c.email,
        from: { email: FROM_EMAIL, name: "Zulema Vázquez" },
        templateId: TEMPLATE_ID,
        asm: {
            groupId: 119472,
        },
    }));

    try {
        await sgMail.send(messages, true);
        console.log(
            `✅ Batch #${batchNumber} enviado con ${contacts.length} correos.`
        );
    } catch (err: any) {
        console.error(
            `❌ Error en batch #${batchNumber}:`,
            err.response?.body || err.message
        );
        logErrorToFile(batchNumber, contacts, err);
    }
};

const main = async () => {
    const contacts = await readCsv("nuevosEmails.csv"); // COLOCAR NOMBRE DEL ARCHIVO CSV ACA!!!
    const batches = chunkArray(contacts, BATCH_SIZE);
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
