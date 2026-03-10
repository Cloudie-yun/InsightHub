const authModal = document.getElementById("auth-modal");
const loginForm = document.getElementById("auth-login");
const signupForm = document.getElementById("auth-signup");
const forgotForm = document.getElementById("auth-forgot");
const resetForm = document.getElementById("auth-reset");
const authTitle = document.getElementById("auth-title");
const authSubtitle = document.getElementById("auth-subtitle");
const userSection = document.getElementById("user-section");
const authSection = document.getElementById("auth-section");
const sidebarUserAvatarInitial = document.getElementById("sidebar-user-avatar-initial");
const sidebarUserName = document.getElementById("sidebar-user-name");
const sidebarUserEmail = document.getElementById("sidebar-user-email");
const userMenuToggle = document.getElementById("user-menu-toggle");
const userMenuDropdown = document.getElementById("user-menu-dropdown");
const userMenuCaret = document.getElementById("user-menu-caret");
const userEditProfileBtn = document.getElementById("user-edit-profile-btn");
const userLogoutBtn = document.getElementById("user-logout-btn");
const emailVerifyBadge = document.getElementById("email-verify-badge");
const emailVerifyBanner = document.getElementById("email-verify-banner");
const emailVerifyResendBtn = document.getElementById("email-verify-resend-btn");
const emailVerifyResendStatus = document.getElementById("email-verify-resend-status");
const emailVerifyResultModal = document.getElementById("email-verify-result-modal");
const emailVerifyResultTitle = document.getElementById("email-verify-result-title");
const emailVerifyResultText = document.getElementById("email-verify-result-text");
const emailVerifyResultIcon = document.getElementById("email-verify-result-icon");
const emailVerifyResultBtn = document.getElementById("email-verify-result-btn");
const loginSubmitBtn = document.getElementById("login-submit-btn");
const loginSubmitText = document.getElementById("login-submit-text");
const loginSubmitSpinner = document.getElementById("login-submit-spinner");
const signupSubmitBtn = document.getElementById("signup-submit-btn");
const signupSubmitText = document.getElementById("signup-submit-text");
const signupSubmitSpinner = document.getElementById("signup-submit-spinner");
const resetSubmitBtn = document.getElementById("reset-submit-btn");
const resetSubmitText = document.getElementById("reset-submit-text");
const resetSubmitSpinner = document.getElementById("reset-submit-spinner");
let currentAuthUser = null;
let isUserMenuOpen = false;

// ========== TOAST NOTIFICATIONS ==========
const toastContainer = document.getElementById("toast-container");
const TOAST_DEFAULT_DURATION_MS = 4500;
const TOAST_TYPE_CONFIG = {
    info: {
        title: "Info",
        iconClass: "fa-solid fa-circle-info",
        borderClass: "border-l-brand-500",
        iconWrapClass: "bg-brand-100 text-brand-700",
    },
    success: {
        title: "Success",
        iconClass: "fa-solid fa-circle-check",
        borderClass: "border-l-emerald-500",
        iconWrapClass: "bg-emerald-100 text-emerald-700",
    },
    warning: {
        title: "Warning",
        iconClass: "fa-solid fa-triangle-exclamation",
        borderClass: "border-l-amber-500",
        iconWrapClass: "bg-amber-100 text-amber-700",
    },
    error: {
        title: "Error",
        iconClass: "fa-solid fa-circle-xmark",
        borderClass: "border-l-red-500",
        iconWrapClass: "bg-red-100 text-red-700",
    },
};

const removeToast = (toastEl) => {
    if (!toastEl || !toastEl.parentElement) return;
    toastEl.classList.remove("opacity-100", "translate-y-0", "scale-100");
    toastEl.classList.add("opacity-0", "-translate-y-2", "scale-[0.98]");
    window.setTimeout(() => {
        toastEl.remove();
    }, 220);
};

const showToast = (input, options = {}) => {
    if (!toastContainer) return;

    const payload = typeof input === "string" ? { message: input, ...options } : (input || {});
    const message = String(payload.message || "").trim();
    if (!message) return;

    const typeKey = TOAST_TYPE_CONFIG[payload.type] ? payload.type : "info";
    const config = TOAST_TYPE_CONFIG[typeKey];
    const title = String(payload.title || config.title);
    const duration = Number(payload.duration || TOAST_DEFAULT_DURATION_MS);

    const toastEl = document.createElement("article");
    toastEl.className = [
        "pointer-events-auto",
        "relative",
        "w-full",
        "rounded-xl",
        "border",
        "border-slate-200",
        "border-l-4",
        config.borderClass,
        "bg-white/95",
        "backdrop-blur",
        "shadow-lg",
        "px-3",
        "py-3",
        "transition-all",
        "duration-200",
        "opacity-0",
        "-translate-y-2",
        "scale-[0.98]",
    ].join(" ");

    const rowEl = document.createElement("div");
    rowEl.className = "flex items-start gap-3";

    const iconWrapEl = document.createElement("div");
    iconWrapEl.className = `mt-0.5 h-8 w-8 shrink-0 rounded-full flex items-center justify-center ${config.iconWrapClass}`;
    const iconEl = document.createElement("i");
    iconEl.className = `${config.iconClass} text-sm`;
    iconWrapEl.appendChild(iconEl);

    const contentEl = document.createElement("div");
    contentEl.className = "min-w-0 flex-1";

    const titleEl = document.createElement("p");
    titleEl.className = "truncate text-sm font-semibold text-slate-900";
    titleEl.textContent = title;

    const messageEl = document.createElement("p");
    messageEl.className = "mt-0.5 text-sm leading-5 text-slate-600";
    messageEl.textContent = message;

    const closeBtn = document.createElement("button");
    closeBtn.type = "button";
    closeBtn.className = "mt-0.5 rounded-md p-1 text-slate-400 hover:bg-slate-100 hover:text-slate-600 transition-colors";
    closeBtn.setAttribute("aria-label", "Dismiss notification");
    closeBtn.innerHTML = '<i class="fa-solid fa-xmark text-xs"></i>';
    closeBtn.addEventListener("click", () => removeToast(toastEl));

    contentEl.appendChild(titleEl);
    contentEl.appendChild(messageEl);
    rowEl.appendChild(iconWrapEl);
    rowEl.appendChild(contentEl);
    rowEl.appendChild(closeBtn);
    toastEl.appendChild(rowEl);
    toastContainer.appendChild(toastEl);

    window.requestAnimationFrame(() => {
        toastEl.classList.remove("opacity-0", "-translate-y-2", "scale-[0.98]");
        toastEl.classList.add("opacity-100", "translate-y-0", "scale-100");
    });

    if (Number.isFinite(duration) && duration > 0) {
        window.setTimeout(() => removeToast(toastEl), duration);
    }
};

window.toast = {
    info: (message, opts = {}) => showToast({ type: "info", message, ...opts }),
    success: (message, opts = {}) => showToast({ type: "success", message, ...opts }),
    warning: (message, opts = {}) => showToast({ type: "warning", message, ...opts }),
    error: (message, opts = {}) => showToast({ type: "error", message, ...opts }),
    show: showToast,
};

document.addEventListener("DOMContentLoaded", () => {
    const message = sessionStorage.getItem("toastMessage");
    const type = sessionStorage.getItem("toastType") || "info";
    if (message) {
        showToast({ type, message });
    }
    sessionStorage.removeItem("toastMessage");
    sessionStorage.removeItem("toastType");
});

// ========== CUSTOM PLACEHOLDER MANAGEMENT ==========
const initCustomPlaceholders = () => {
    const inputs = document.querySelectorAll('#auth-modal input[type="text"], #auth-modal input[type="email"], #auth-modal input[type="password"]');
    
    inputs.forEach(input => {
        const container = input.parentElement;
        const placeholder = container.querySelector('.custom-placeholder');
        
        if (!placeholder) return;
        
        const updatePlaceholder = () => {
            if (input.value.length > 0) {
                placeholder.style.opacity = '0';
                placeholder.style.visibility = 'hidden';
            } else {
                placeholder.style.opacity = '1';
                placeholder.style.visibility = 'visible';
            }
        };
        
        input.addEventListener('focus', () => {
            placeholder.style.opacity = '0';
            placeholder.style.visibility = 'hidden';
        });
        
        input.addEventListener('blur', () => {
            if (input.value.length === 0) {
                placeholder.style.opacity = '1';
                placeholder.style.visibility = 'visible';
            }
        });
        
        input.addEventListener('input', updatePlaceholder);
        updatePlaceholder();
    });
};

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCustomPlaceholders);
} else {
    initCustomPlaceholders();
}

// ========== FORM UTILITIES ==========
const clearFormErrors = (formId) => {
    document.querySelectorAll(`#${formId} span[id$='-error']`).forEach((el) => {
        el.classList.add("hidden");
    });
    document.querySelectorAll(`#${formId} input`).forEach((input) => {
        input.classList.remove("auth-input-error");
    });
};

const resetAuthFormState = (formId) => {
    const form = document.getElementById(formId);
    if (!form) return;
    form.reset();
    clearFormErrors(formId);
    
    form.querySelectorAll("input[type='text']").forEach((input) => {
        if (input.id.toLowerCase().includes("password")) {
            input.type = "password";
        }
    });
    
    form.querySelectorAll('.custom-placeholder').forEach(placeholder => {
        placeholder.style.opacity = '1';
        placeholder.style.visibility = 'visible';
    });
};

const resetPasswordToggleIcons = () => {
    document.querySelectorAll(".auth-password-toggle").forEach((button) => {
        button.innerHTML = '<i class="fa-solid fa-eye"></i>';
        button.setAttribute("aria-label", "Show password");
    });
};

const resetAuthForms = () => {
    resetAuthFormState("auth-login");
    resetAuthFormState("auth-signup");
    resetAuthFormState("auth-forgot");
    resetAuthFormState("auth-reset");
    resetPasswordToggleIcons();
    
    const signupDropdown = document.getElementById('password-strength-dropdown');
    if (signupDropdown) signupDropdown.classList.add('hidden');
    const resetDropdown = document.getElementById('reset-password-strength-dropdown');
    if (resetDropdown) resetDropdown.classList.add('hidden');
    resetPasswordStrengthIndicators("signup");
    resetPasswordStrengthIndicators("reset");

    resetForgotPasswordStep();
    setLoginSubmitLoading(false);
    setSignupSubmitLoading(false);
    setForgotSubmitLoading(false);
    setResetSubmitLoading(false);
};

const setSignupSubmitLoading = (isLoading) => {
    if (!signupSubmitBtn || !signupSubmitText || !signupSubmitSpinner) return;
    signupSubmitBtn.disabled = isLoading;
    signupSubmitBtn.setAttribute("aria-busy", isLoading ? "true" : "false");
    signupSubmitText.textContent = isLoading ? "Signing you up..." : "Sign Up";
    signupSubmitSpinner.classList.toggle("hidden", !isLoading);
};

const setLoginSubmitLoading = (isLoading) => {
    if (!loginSubmitBtn || !loginSubmitText || !loginSubmitSpinner) return;
    loginSubmitBtn.disabled = isLoading;
    loginSubmitBtn.setAttribute("aria-busy", isLoading ? "true" : "false");
    loginSubmitText.textContent = isLoading ? "Logging you in..." : "Log In";
    loginSubmitSpinner.classList.toggle("hidden", !isLoading);
};

const setForgotSubmitLoading = (isLoading) => {
    if (!forgotSubmitBtn || !forgotSubmitText || !forgotSubmitSpinner) return;
    forgotSubmitBtn.disabled = isLoading;
    forgotSubmitBtn.setAttribute("aria-busy", isLoading ? "true" : "false");
    forgotSubmitText.textContent = isLoading ? "Sending reset link..." : forgotSubmitDefaultText;
    forgotSubmitSpinner.classList.toggle("hidden", !isLoading);
};

const setResetSubmitLoading = (isLoading) => {
    if (!resetSubmitBtn || !resetSubmitText || !resetSubmitSpinner) return;
    resetSubmitBtn.disabled = isLoading;
    resetSubmitBtn.setAttribute("aria-busy", isLoading ? "true" : "false");
    resetSubmitText.textContent = isLoading ? "Resetting password..." : "Reset Password";
    resetSubmitSpinner.classList.toggle("hidden", !isLoading);
};

const setEmailVerifyResendStatus = (message, isError = false) => {
    if (!emailVerifyResendStatus) return;
    emailVerifyResendStatus.textContent = message || "";
    emailVerifyResendStatus.classList.remove("text-emerald-700", "text-red-700");
    if (message) {
        emailVerifyResendStatus.classList.add(isError ? "text-red-700" : "text-emerald-700");
    }
};

const setUserMenuOpen = (open) => {
    if (!userMenuToggle || !userMenuDropdown) return;
    isUserMenuOpen = open;
    userMenuDropdown.classList.toggle("hidden", !open);
    userMenuToggle.setAttribute("aria-expanded", open ? "true" : "false");
    if (userMenuCaret) {
        userMenuCaret.classList.toggle("rotate-180", open);
    }
};

// ========== AUTH MODAL CONTROLS ==========
const setAuthMode = (mode) => {
    resetAuthForms();
    const isLogin = mode === "login";
    const isSignup = mode === "signup";
    const isForgot = mode === "forgot";
    const isReset = mode === "reset";

    loginForm.classList.toggle("hidden", !isLogin);
    signupForm.classList.toggle("hidden", !isSignup);
    forgotForm.classList.toggle("hidden", !isForgot);
    resetForm.classList.toggle("hidden", !isReset);

    if (isLogin) {
        authTitle.textContent = "Welcome Back";
        authSubtitle.textContent = "Log in to continue";
    } else if (isSignup) {
        authTitle.textContent = "Create Your Account";
        authSubtitle.textContent = "Sign up in seconds";
    } else if (isReset) {
        authTitle.textContent = "Reset Password";
        authSubtitle.textContent = "Set a new password for your account";
    } else {
        authTitle.textContent = "Forgot Password";
        authSubtitle.textContent = "Enter your email to receive a reset link";
    }

    setTimeout(initCustomPlaceholders, 10);
};

const openAuth = (mode) => {
    setAuthMode(mode);
    authModal.classList.remove("hidden");
    authModal.classList.add("flex");
};

const closeAuth = () => {
    resetAuthForms();
    authModal.classList.add("hidden");
    authModal.classList.remove("flex");
};

document.querySelectorAll("[data-auth-open]").forEach((el) => {
    el.addEventListener("click", () => openAuth(el.dataset.authOpen));
});

document.querySelectorAll("[data-google-auth]").forEach((el) => {
    el.addEventListener("click", () => {
        const next = window.location.pathname;
        window.location.href = `/api/auth/google/start?next=${encodeURIComponent(next)}`;
    });
});

document.querySelectorAll("[data-auth-close]").forEach((el) => {
    el.addEventListener("click", closeAuth);
});

document.addEventListener("keydown", (e) => {
    if (e.key !== "Escape") return;
    closeAuth();
    closeEmailVerifyResultModal();
    setUserMenuOpen(false);
});

const openEmailVerifyResultModal = (status) => {
    if (!emailVerifyResultModal) return;

    if (status === "success") {
        emailVerifyResultTitle.textContent = "Email Verified";
        emailVerifyResultText.textContent = "Congratulations, your email has been verified. Please enjoy InsightHub.";
        emailVerifyResultIcon.className = "fa-solid fa-circle-check text-2xl";
        emailVerifyResultIcon.parentElement.className =
            "h-14 w-14 rounded-full bg-emerald-100 text-emerald-700 flex items-center justify-center mx-auto mb-4";
    } else if (status === "invalid") {
        emailVerifyResultTitle.textContent = "Link Expired";
        emailVerifyResultText.textContent = "This verification link is invalid or expired. Please request a new verification email.";
        emailVerifyResultIcon.className = "fa-solid fa-link-slash text-2xl";
        emailVerifyResultIcon.parentElement.className =
            "h-14 w-14 rounded-full bg-amber-100 text-amber-700 flex items-center justify-center mx-auto mb-4";
    } else {
        emailVerifyResultTitle.textContent = "Verification Failed";
        emailVerifyResultText.textContent = "We could not verify your email right now. Please try again later.";
        emailVerifyResultIcon.className = "fa-solid fa-triangle-exclamation text-2xl";
        emailVerifyResultIcon.parentElement.className =
            "h-14 w-14 rounded-full bg-red-100 text-red-700 flex items-center justify-center mx-auto mb-4";
    }

    emailVerifyResultModal.classList.remove("hidden");
    emailVerifyResultModal.classList.add("flex");
};

const closeEmailVerifyResultModal = () => {
    if (!emailVerifyResultModal) return;
    emailVerifyResultModal.classList.add("hidden");
    emailVerifyResultModal.classList.remove("flex");
};

document.querySelectorAll("[data-email-verify-close]").forEach((el) => {
    el.addEventListener("click", closeEmailVerifyResultModal);
});
if (emailVerifyResultBtn) {
    emailVerifyResultBtn.addEventListener("click", closeEmailVerifyResultModal);
}

// ========== VALIDATION ==========
const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const strongPasswordRegex = /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[^A-Za-z\d]).{8,}$/;

const showError = (input, errorId, show) => {
    const error = document.getElementById(errorId);
    if (!error) return;
    error.classList.toggle("hidden", !show);
    if (input) {
        input.classList.toggle("auth-input-error", show);
    }
};

// ========== FORGOT PASSWORD FLOW ==========
const forgotEmailInput = document.getElementById("forgot-email");
const forgotSubmitBtn = document.getElementById("forgot-submit-btn");
const forgotSubmitText = document.getElementById("forgot-submit-text");
const forgotSubmitSpinner = document.getElementById("forgot-submit-spinner");
const forgotSuccess = document.getElementById("forgot-success");
let forgotSubmitDefaultText = "Send Reset Link";

const setAuthUiState = (user) => {
    currentAuthUser = user || null;
    const isLoggedIn = Boolean(user && user.user_id);
    userSection.classList.toggle("hidden", !isLoggedIn);
    authSection.classList.toggle("hidden", isLoggedIn);

    if (!isLoggedIn) {
        setUserMenuOpen(false);
        if (emailVerifyBanner) emailVerifyBanner.classList.add("hidden");
        if (emailVerifyBadge) emailVerifyBadge.classList.add("hidden");
        setEmailVerifyResendStatus("");
        return;
    }

    const username = (user.username || "").trim();
    const email = (user.email || "").trim();
    const initial = username ? username.charAt(0).toUpperCase() : "U";
    const isVerified = Boolean(user.email_verified);

    if (sidebarUserAvatarInitial) sidebarUserAvatarInitial.textContent = initial;
    sidebarUserName.textContent = username || "User";
    sidebarUserEmail.textContent = email;
    if (emailVerifyBanner) emailVerifyBanner.classList.toggle("hidden", isVerified);
    if (emailVerifyBadge) emailVerifyBadge.classList.toggle("hidden", isVerified);
    if (isVerified) setEmailVerifyResendStatus("");
};

if (userMenuToggle && userMenuDropdown) {
    userMenuToggle.addEventListener("click", () => {
        setUserMenuOpen(!isUserMenuOpen);
    });

    document.addEventListener("click", (e) => {
        if (!isUserMenuOpen) return;
        if (userSection && !userSection.contains(e.target)) {
            setUserMenuOpen(false);
        }
    });
}

if (userEditProfileBtn) {
    userEditProfileBtn.addEventListener("click", async () => {
        if (!currentAuthUser) return;
        const currentName = (currentAuthUser.username || "").trim();
        const nextName = (window.prompt("Enter your new display name:", currentName) || "").trim();

        if (!nextName || nextName === currentName) {
            setUserMenuOpen(false);
            return;
        }

        try {
            const { response, payload } = await postJson("/api/auth/profile", { username: nextName });
            if (!response.ok) {
                window.alert(payload.error || "Unable to update profile right now.");
                return;
            }
            setAuthFromPayload(payload);
            window.alert("Profile updated.");
        } catch (error) {
            window.alert("Network error. Please try again.");
        } finally {
            setUserMenuOpen(false);
        }
    });
}

if (userLogoutBtn) {
    userLogoutBtn.addEventListener("click", async () => {
        try {
            const { response, payload } = await postJson("/api/auth/logout");
            if (!response.ok) {
                window.alert(payload.error || "Unable to log out right now.");
                return;
            }

            setAuthUiState(null);
            setUserMenuOpen(false);

            sessionStorage.setItem("toastMessage", "You have logged out successfully.");
            sessionStorage.setItem("toastType", "success");

            window.location.href = "/dashboard";
        } catch (error) {
            window.alert("Network error. Please try again.");
        }
    });
}

if (emailVerifyResendBtn) {
    emailVerifyResendBtn.addEventListener("click", async () => {
        if (!currentAuthUser || currentAuthUser.email_verified) return;

        emailVerifyResendBtn.disabled = true;
        setEmailVerifyResendStatus("Sending...");

        try {
            const { response, payload } = await postJson("/api/auth/resend-verification");
            if (!response.ok) {
                setEmailVerifyResendStatus(payload.error || "Failed to resend email.", true);
                return;
            }
            setEmailVerifyResendStatus(payload.message || "Verification email sent.");
        } catch (error) {
            setEmailVerifyResendStatus("Network error. Please try again.", true);
        } finally {
            emailVerifyResendBtn.disabled = false;
        }
    });
}

const resetForgotPasswordStep = () => {
    forgotSubmitDefaultText = "Send Reset Link";
    if (forgotSubmitText) forgotSubmitText.textContent = forgotSubmitDefaultText;
    if (forgotSuccess) forgotSuccess.classList.add("hidden");
};

const hideAllErrorsForInput = (input) => {
    if (!input) return;
    const inputId = input.id;
    document.querySelectorAll(`span[id^="${inputId}-"]`).forEach(error => {
        error.classList.add("hidden");
    });
    input.classList.remove("auth-input-error");
};

const setFieldError = (input, errorId, message = "") => {
    showError(input, errorId, true);
    if (!message) return;
    const errorEl = document.getElementById(errorId);
    if (errorEl) errorEl.textContent = message;
};

const normalizeAuthUser = (user) => ({
    ...(user || {}),
    email_verified: Boolean(user?.email_verified),
});

const setAuthFromPayload = (payload) => {
    setAuthUiState(normalizeAuthUser(payload?.user));
};

const postJson = async (url, body) => {
    const requestOptions = {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
    };
    if (body !== undefined) {
        requestOptions.body = JSON.stringify(body);
    }
    const response = await fetch(url, requestOptions);
    let payload = {};
    try {
        payload = await response.json();
    } catch (error) {
        payload = {};
    }
    return { response, payload };
};

const bindHideErrorsOnFocus = (inputs) => {
    inputs.filter(Boolean).forEach((input) => {
        input.addEventListener("focus", () => hideAllErrorsForInput(input));
    });
};

const bindLiveEmailValidation = (input, errorId) => {
    if (!input) return;
    input.addEventListener("input", () => {
        const email = input.value.trim();
        if (!email || emailRegex.test(email)) {
            hideAllErrorsForInput(input);
            return;
        }
        setFieldError(input, errorId, "Please enter a valid email.");
    });
};

const bindPasswordPairValidation = ({
    passwordInput,
    confirmInput,
    flow,
    dropdown,
    matchErrorId,
}) => {
    if (!passwordInput || !confirmInput) return;

    passwordInput.addEventListener("input", () => {
        const password = passwordInput.value;
        const confirmPassword = confirmInput.value;

        if (!password) {
            hideAllErrorsForInput(passwordInput);
            resetPasswordStrengthIndicators(flow);
        } else {
            const isStrong = checkPasswordStrength(password, flow);
            if (isStrong && dropdown) {
                setTimeout(() => {
                    if (document.activeElement !== passwordInput) {
                        dropdown.classList.add("hidden");
                    }
                }, 1000);
            }
            hideAllErrorsForInput(passwordInput);
        }

        if (confirmPassword.length > 0) {
            if (password !== confirmPassword) {
                setFieldError(confirmInput, matchErrorId);
                return;
            }
            hideAllErrorsForInput(confirmInput);
        }
    });

    confirmInput.addEventListener("input", () => {
        const password = passwordInput.value;
        const confirmPassword = confirmInput.value;
        if (!confirmPassword || password === confirmPassword) {
            hideAllErrorsForInput(confirmInput);
            return;
        }
        setFieldError(confirmInput, matchErrorId);
    });
};

// ========== PASSWORD STRENGTH INDICATOR ==========
const passwordStrengthDropdown = document.getElementById('password-strength-dropdown');
const signupPasswordInput = document.getElementById('signup-password');
const signupPasswordField = signupPasswordInput ? signupPasswordInput.closest('.relative') : null;
const resetPasswordStrengthDropdown = document.getElementById('reset-password-strength-dropdown');
const resetPasswordInput = document.getElementById('reset-password');
const resetPasswordField = resetPasswordInput ? resetPasswordInput.closest('.relative') : null;

const passwordRequirementIdsByFlow = {
    signup: ['pwd-length', 'pwd-uppercase', 'pwd-lowercase', 'pwd-number', 'pwd-special'],
    reset: ['reset-pwd-length', 'reset-pwd-uppercase', 'reset-pwd-lowercase', 'reset-pwd-number', 'reset-pwd-special'],
};

const updatePasswordStrengthIndicator = (requirement, isValid) => {
    const element = document.getElementById(requirement);
    if (!element) return;
    
    const icon = element.querySelector('i');
    const text = element.querySelector('span');
    
    if (isValid) {
        icon.classList.remove('fa-xmark', 'text-red-500');
        icon.classList.add('fa-check', 'text-green-500');
        text.classList.remove('text-slate-500');
        text.classList.add('text-green-600');
    } else {
        icon.classList.remove('fa-check', 'text-green-500');
        icon.classList.add('fa-xmark', 'text-red-500');
        text.classList.remove('text-green-600');
        text.classList.add('text-slate-500');
    }
};

const resetPasswordStrengthIndicators = (flow = "signup") => {
    (passwordRequirementIdsByFlow[flow] || []).forEach(req => {
        updatePasswordStrengthIndicator(req, false);
    });
};

const checkPasswordStrength = (password, flow = "signup") => {
    const hasLength = password.length >= 8;
    const hasUppercase = /[A-Z]/.test(password);
    const hasLowercase = /[a-z]/.test(password);
    const hasNumber = /\d/.test(password);
    const hasSpecial = /[^A-Za-z\d]/.test(password);

    const requirementIds = passwordRequirementIdsByFlow[flow] || [];
    updatePasswordStrengthIndicator(requirementIds[0], hasLength);
    updatePasswordStrengthIndicator(requirementIds[1], hasUppercase);
    updatePasswordStrengthIndicator(requirementIds[2], hasLowercase);
    updatePasswordStrengthIndicator(requirementIds[3], hasNumber);
    updatePasswordStrengthIndicator(requirementIds[4], hasSpecial);
    
    return hasLength && hasUppercase && hasLowercase && hasNumber && hasSpecial;
};

// Show/hide dropdown based on hover, not focus.
if (signupPasswordField && passwordStrengthDropdown) {
    signupPasswordField.addEventListener('mouseenter', () => {
        passwordStrengthDropdown.classList.remove('hidden');
        hideAllErrorsForInput(signupPasswordInput);
    });

    signupPasswordField.addEventListener('mouseleave', () => {
        passwordStrengthDropdown.classList.add('hidden');
    });
}

if (resetPasswordField && resetPasswordStrengthDropdown) {
    resetPasswordField.addEventListener('mouseenter', () => {
        resetPasswordStrengthDropdown.classList.remove('hidden');
        hideAllErrorsForInput(resetPasswordInput);
    });

    resetPasswordField.addEventListener('mouseleave', () => {
        resetPasswordStrengthDropdown.classList.add('hidden');
    });
}

// ========== PASSWORD TOGGLE ==========
document.querySelectorAll(".auth-password-toggle").forEach((button) => {
    button.addEventListener("click", () => {
        const input = document.getElementById(button.dataset.target);
        if (!input) return;

        const isHidden = input.type === "password";
        input.type = isHidden ? "text" : "password";
        button.innerHTML = isHidden
            ? '<i class="fa-solid fa-eye-slash"></i>'
            : '<i class="fa-solid fa-eye"></i>';
        button.setAttribute("aria-label", isHidden ? "Hide password" : "Show password");
    });
});

// ========== REAL-TIME VALIDATION ==========
const loginEmailInput = document.getElementById("login-email");
const loginPasswordInput = document.getElementById("login-password");
const signupNameInput = document.getElementById("signup-name");
const signupEmailInput = document.getElementById("signup-email");
const signupConfirmPasswordInput = document.getElementById("signup-confirm-password");
const resetConfirmPasswordInput = document.getElementById("reset-confirm-password");

bindHideErrorsOnFocus([
    loginEmailInput,
    loginPasswordInput,
    signupNameInput,
    signupEmailInput,
    signupConfirmPasswordInput,
    resetPasswordInput,
    resetConfirmPasswordInput,
    forgotEmailInput,
]);

if (loginPasswordInput) {
    loginPasswordInput.addEventListener("input", () => {
        hideAllErrorsForInput(loginPasswordInput);
    });
}

if (signupNameInput) {
    signupNameInput.addEventListener("input", () => {
        hideAllErrorsForInput(signupNameInput);
    });
}

bindLiveEmailValidation(loginEmailInput, "login-email-error");
bindLiveEmailValidation(signupEmailInput, "signup-email-error");
bindLiveEmailValidation(forgotEmailInput, "forgot-email-error");

bindPasswordPairValidation({
    passwordInput: signupPasswordInput,
    confirmInput: signupConfirmPasswordInput,
    flow: "signup",
    dropdown: passwordStrengthDropdown,
    matchErrorId: "signup-password-match-error",
});
bindPasswordPairValidation({
    passwordInput: resetPasswordInput,
    confirmInput: resetConfirmPasswordInput,
    flow: "reset",
    dropdown: resetPasswordStrengthDropdown,
    matchErrorId: "reset-password-match-error",
});

// ========== LOGIN FORM SUBMISSION ==========
loginForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    clearFormErrors("auth-login");

    const email = loginEmailInput.value.trim();
    const password = loginPasswordInput.value;
    let isValid = true;

    if (!email) {
        setFieldError(loginEmailInput, "login-email-error", "Please enter your email.");
        isValid = false;
    } else if (!emailRegex.test(email)) {
        setFieldError(loginEmailInput, "login-email-error", "Please enter a valid email.");
        isValid = false;
    }

    if (!password.trim()) {
        setFieldError(loginPasswordInput, "login-password-error");
        isValid = false;
    }

    if (!isValid) return;

    setLoginSubmitLoading(true);
    try {
        const { response, payload } = await postJson("/api/auth/login", { email, password });
        if (!response.ok) {
            if (response.status === 401) {
                setFieldError(loginPasswordInput, "login-password-error", payload.error || "Invalid email or password.");
                return;
            }

            setFieldError(loginEmailInput, "login-email-error", payload.error || "Login failed. Please try again.");
            return;
        }

        setAuthFromPayload(payload);
        closeAuth();
    } catch (error) {
        setFieldError(loginEmailInput, "login-email-error", "Network error. Please try again.");
    } finally {
        setLoginSubmitLoading(false);
    }
});

// ========== SIGNUP FORM SUBMISSION ==========
signupForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    clearFormErrors("auth-signup");

    const name = signupNameInput.value.trim();
    const email = signupEmailInput.value.trim();
    const password = signupPasswordInput.value;
    const confirmPassword = signupConfirmPasswordInput.value;
    let isValid = true;

    if (!name) {
        setFieldError(signupNameInput, "signup-name-error");
        isValid = false;
    }

    if (!email) {
        setFieldError(signupEmailInput, "signup-email-error", "Please enter your email.");
        isValid = false;
    } else if (!emailRegex.test(email)) {
        setFieldError(signupEmailInput, "signup-email-error", "Please enter a valid email.");
        isValid = false;
    }

    if (!password.trim()) {
        setFieldError(signupPasswordInput, "signup-password-error");
        isValid = false;
    } else if (!strongPasswordRegex.test(password)) {
        setFieldError(signupPasswordInput, "signup-password-strength-error");
        isValid = false;
    }

    if (!confirmPassword.trim()) {
        setFieldError(signupConfirmPasswordInput, "signup-confirm-password-error");
        isValid = false;
    } else if (password !== confirmPassword) {
        setFieldError(signupConfirmPasswordInput, "signup-password-match-error");
        isValid = false;
    }

    if (!isValid) return;

    setSignupSubmitLoading(true);
    try {
        const { response, payload } = await postJson("/api/auth/signup", {
            username: name,
            email,
            password,
        });
        if (!response.ok) {
            if (response.status === 409) {
                setFieldError(signupEmailInput, "signup-email-error", payload.error || "An account with this email already exists.");
                return;
            }

            setFieldError(signupEmailInput, "signup-email-error", payload.error || "Signup failed. Please try again.");
            return;
        }

        setAuthFromPayload(payload);
        closeAuth();
    } catch (error) {
        setFieldError(signupEmailInput, "signup-email-error", "Network error. Please try again.");
    } finally {
        setSignupSubmitLoading(false);
    }
});

// ========== FORGOT PASSWORD FORM SUBMISSION ==========
forgotForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    clearFormErrors("auth-forgot");
    if (forgotSuccess) forgotSuccess.classList.add("hidden");

    const email = forgotEmailInput.value.trim();
    let isValid = true;

    if (!email) {
        setFieldError(forgotEmailInput, "forgot-email-error", "Please enter your email.");
        isValid = false;
    } else if (!emailRegex.test(email)) {
        setFieldError(forgotEmailInput, "forgot-email-error", "Please enter a valid email.");
        isValid = false;
    }

    if (!isValid) return;

    setForgotSubmitLoading(true);
    try {
        const { response, payload } = await postJson("/api/auth/forgot-password/request", { email });

        if (!response.ok) {
            setFieldError(forgotEmailInput, "forgot-email-error", payload.error || "Unable to send reset link. Please try again.");
            return;
        }

        if (forgotSuccess) forgotSuccess.classList.remove("hidden");
        forgotSubmitDefaultText = "Resend Reset Link";
        if (forgotSubmitText) forgotSubmitText.textContent = forgotSubmitDefaultText;
    } catch (error) {
        setFieldError(forgotEmailInput, "forgot-email-error", "Network error. Please try again.");
    } finally {
        setForgotSubmitLoading(false);
    }
});

resetForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    clearFormErrors("auth-reset");

    const password = resetPasswordInput.value;
    const confirmPassword = resetConfirmPasswordInput.value;
    let isValid = true;

    if (!password.trim()) {
        setFieldError(resetPasswordInput, "reset-password-error");
        isValid = false;
    } else if (!strongPasswordRegex.test(password)) {
        setFieldError(resetPasswordInput, "reset-password-strength-error");
        isValid = false;
    }

    if (!confirmPassword.trim()) {
        setFieldError(resetConfirmPasswordInput, "reset-confirm-password-error");
        isValid = false;
    } else if (password !== confirmPassword) {
        setFieldError(resetConfirmPasswordInput, "reset-password-match-error");
        isValid = false;
    }

    if (!isValid) return;

    setResetSubmitLoading(true);
    try {
        const { response, payload } = await postJson("/api/auth/forgot-password/reset", { new_password: password });

        if (!response.ok) {
            setFieldError(
                resetPasswordInput,
                "reset-password-error",
                payload.error || "Unable to reset password. Please request a new reset link."
            );
            return;
        }

        setAuthFromPayload(payload);
        closeAuth();
        sessionStorage.setItem("toastMessage", "Password changed successfully. You are now logged in.");
        sessionStorage.setItem("toastType", "success");
        window.location.href = "/dashboard";
    } catch (error) {
        setFieldError(resetPasswordInput, "reset-password-error", "Network error. Please try again.");
    } finally {
        setResetSubmitLoading(false);
    }
});

const SIDEBAR_STATE_KEY = "insighthub.sidebarCollapsed";
const CONVO_STATE_KEY = "insighthub.convoOpen";
const THEME_STATE_KEY = "insighthub.theme";

// Sidebar toggle
const sidebarToggle = document.getElementById("sidebar-toggle");

// Conversations submenu toggle
const convoToggle = document.getElementById("convo-toggle");
const convoSubmenu = document.getElementById("convo-submenu");
const convoCaret = document.getElementById("convo-caret");
const themeToggle = document.getElementById("theme-toggle");

const syncConvoCaret = () => {
    const isOpen = !convoSubmenu.classList.contains("hidden");
    convoCaret.classList.toggle("rotate-180", isOpen);
};

const syncThemeOptions = (theme) => {
    if (!themeToggle) return;
    themeToggle.checked = theme !== "dark";
};

const applyTheme = (theme, persist = true) => {
    const nextTheme = theme === "dark" ? "dark" : "light";
    document.body.classList.toggle("theme-dark", nextTheme === "dark");
    document.body.classList.toggle("theme-light", nextTheme !== "dark");
    syncThemeOptions(nextTheme);
    if (persist) {
        localStorage.setItem(THEME_STATE_KEY, nextTheme);
    }
};

// Restore persisted UI state on refresh
const isSidebarCollapsed = localStorage.getItem(SIDEBAR_STATE_KEY) === "true";
if (isSidebarCollapsed) {
    document.body.classList.add("sidebar-collapsed");
}

const savedConvoOpen = localStorage.getItem(CONVO_STATE_KEY);
if (savedConvoOpen === "false") {
    convoSubmenu.classList.add("hidden");
} else if (savedConvoOpen === "true") {
    convoSubmenu.classList.remove("hidden");
}

const savedTheme = localStorage.getItem(THEME_STATE_KEY);
applyTheme(savedTheme === "dark" ? "dark" : "light", false);
syncConvoCaret();

const params = new URLSearchParams(window.location.search);
const emailVerifiedStatus = params.get("email_verified");
const passwordResetStatus = params.get("pwd_reset");
const googleAuthStatus = params.get("google_auth");
if (emailVerifiedStatus) {
    openEmailVerifyResultModal(emailVerifiedStatus);
    params.delete("email_verified");
}
if (googleAuthStatus === "success") {
    showToast({ type: "success", title: "Signed In", message: "Signed in with Google." });
    params.delete("google_auth");
} else if (googleAuthStatus === "conflict") {
    showToast({
        type: "warning",
        title: "Use Existing Sign-In",
        message: "This email already has a password sign-in. Please log in with email and password.",
        duration: 6000,
    });
    openAuth("login");
    params.delete("google_auth");
} else if (googleAuthStatus === "error") {
    showToast({ type: "error", title: "Google Sign-In Failed", message: "Please try again." });
    openAuth("login");
    params.delete("google_auth");
}
if (passwordResetStatus === "verified") {
    openAuth("reset");
} else if (passwordResetStatus === "invalid") {
    openAuth("forgot");
    showError(forgotEmailInput, "forgot-email-error", true);
    document.getElementById("forgot-email-error").textContent =
        "Reset link is invalid or expired. Request a new one.";
} else if (passwordResetStatus === "error") {
    openAuth("forgot");
    showError(forgotEmailInput, "forgot-email-error", true);
    document.getElementById("forgot-email-error").textContent =
        "Unable to verify reset link right now. Please try again.";
}
params.delete("pwd_reset");
const query = params.toString();
const nextUrl = `${window.location.pathname}${query ? `?${query}` : ""}${window.location.hash}`;
window.history.replaceState({}, "", nextUrl);

sidebarToggle.addEventListener("click", () => {
    setUserMenuOpen(false);
    document.body.classList.toggle("sidebar-collapsed");
    localStorage.setItem(
        SIDEBAR_STATE_KEY,
        document.body.classList.contains("sidebar-collapsed")
    );
});

convoToggle.addEventListener("click", () => {
    const isHidden = convoSubmenu.classList.toggle("hidden");
    localStorage.setItem(CONVO_STATE_KEY, (!isHidden).toString());
    syncConvoCaret();
});

if (themeToggle) {
    themeToggle.addEventListener("change", () => {
        applyTheme(themeToggle.checked ? "light" : "dark");
    });
}

setAuthUiState(window.__AUTH_USER__ || null);
